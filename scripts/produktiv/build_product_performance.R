# ============================================================================
# build_product_performance.R
# ============================================================================
# Baut das Serving-Bundle fuer den Dashboard-Tab "Produktperformance":
# Produktfamilien-Mapping + attribuierter Umsatz + allozierter Werbe-Spend
# je Klicktag x Produktfamilie x Kanal.
#
# WARUM VORBERECHNET: die Quelle sind die Adtribute-Parquets (~22 GB, lokal).
# Die liegen auf connect.posit.cloud nicht vor, und der Aufbau dauert ~1-2 min
# -- beides verbietet eine Live-Abfrage aus der Shiny-App.
#
# ---------------------------------------------------------------------------
# RECHENWEG (und seine Grenzen -- bitte im Tab genauso kommunizieren)
#
# 1. In Adtribute ist `conversion_id` GENAU EIN PRODUKT einer Bestellung
#    (geprueft: nie mehr als ein `product_title_with_variant` je conversion_id).
#    Damit ist Attribution auf Produktebene ueberhaupt erst moeglich.
# 2. `gross_revenue` ist ueber alle Touchpoint-Zeilen einer conversion
#    REPLIZIERT -> je conversion einmal per MAX() ziehen, nie summieren.
# 3. Die Gewichte werden je conversion auf 1 normiert. Summe der attribuierten
#    Umsaetze == Summe der Conversion-Umsaetze (wird unten geprueft).
# 4. Spend ist je (Tag, Kampagne) gebucht, NICHT je Produkt. Er wird deshalb
#    ALLOZIERT, nicht gemessen:
#      Stufe 1: innerhalb (Klicktag, Kanal, Kampagne) nach Umsatzanteil des
#               Produkts. Das traegt, weil die Kampagnen produktspezifisch
#               benannt sind -- der Produktname steht im Kampagnennamen.
#      Stufe 2: Spend ohne Kampagnen-Treffer -> nach Umsatzanteil des Produkts
#               am GESAMTEN Tag, bewusst nicht nach dem Eigen-Mix des Kanals.
#               Ohne Kampagnen-Treffer gibt es keine produktspezifische
#               Information; Adtributes Sammelkanal "Catch All" traegt zudem
#               einen erheblichen Teil des Spends bei fast keinen Conversions
#               und waere als Verteilschluessel irrefuehrend.
#      Rest:    Spend an Tagen ganz ohne attribuierten Umsatz bleibt NICHT
#               zugeordnet und wird separat ausgewiesen -- getrennt nach
#               "nach Datenende" (Sync-Rueckstand) und "ohne Attribution".
# 5. Alles auf KLICK-Datum (`channel_date`). Nur so passen Spend und Umsatz
#    zusammen. Die Shopify-Kennzahlen des Tabs laufen auf Bestelldatum -- die
#    beiden Bloecke sind deshalb bewusst getrennt beschriftet.
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(DBI)
  library(duckdb)
  library(tibble)
})

PP_TP_GLOB <- "~/data/adtribute_parquet_chunks_full/*.parquet"
PP_SP_GLOB <- "~/data/adtribute_spend_chunks/*.parquet"
PP_MART <- "~/git/dashboard/data/shopify.duckdb"
PP_OUT <- "~/git/dashboard/data/product_performance.rds"
PP_FAM_R <- "~/git/dieseoR/scripts/produktiv/produkt_familien.R"
PP_OVERRIDE <- "~/git/dieseoR/scripts/produktiv/produkt_familien_override.csv"
# Attributionsmodell: gleiche Wahl wie im Ads-Dashboard (app.R default).
PP_MODELL <- "weight_pammys_opt"

source(PP_FAM_R)

message("[", Sys.time(), "] Produktperformance-Bundle: Start")

# --- 1) Familien-Mapping aus dem Data Mart ----------------------------------
# Titel aus BEIDEN Quellen: der Shopify-Mart kennt die volle Historie, die
# Adtribute-Parquets koennen Titel fuehren, die im Mart (noch) nicht auftauchen.
# Ein Titel ohne Familie wuerde sonst still aus dem Marketing-Block fallen.
mart <- DBI::dbConnect(duckdb::duckdb(), dbdir = path.expand(PP_MART), read_only = TRUE)
titel_shopify <- tryCatch(
  DBI::dbGetQuery(
    mart,
    "SELECT DISTINCT product_title FROM orders
     WHERE product_title IS NOT NULL AND product_title <> ''"
  )$product_title,
  finally = DBI::dbDisconnect(mart, shutdown = TRUE)
)

tmp <- DBI::dbConnect(duckdb::duckdb())
titel_adtribute <- tryCatch(
  DBI::dbGetQuery(tmp, sprintf(
    "SELECT DISTINCT product_title FROM read_parquet(%s)
     WHERE product_title IS NOT NULL",
    DBI::dbQuoteString(tmp, path.expand(PP_TP_GLOB))
  ))$product_title,
  finally = DBI::dbDisconnect(tmp, shutdown = TRUE)
)

familien <- pf_map(union(titel_shopify, titel_adtribute),
  override_pfad = path.expand(PP_OVERRIDE)
)

n_offen <- sum(familien$produkt_key == "unbekannt")
if (n_offen > 0) {
  warning(
    "Produktfamilien: ", n_offen, " Titel ohne Zuordnung -> ",
    "Regel in produkt_familien.R ergaenzen oder Override pflegen: ",
    paste(utils::head(familien$product_title[familien$produkt_key == "unbekannt"], 10),
      collapse = " | "
    )
  )
}
message(
  "  Familien-Mapping: ", nrow(familien), " Titel -> ",
  dplyr::n_distinct(familien$produkt_key), " Familien (", n_offen, " offen)"
)

# --- 2) Adtribute: Attribution + Spend-Allokation in DuckDB -----------------
con <- DBI::dbConnect(duckdb::duckdb())
ergebnis <- tryCatch(
  {
    DBI::dbExecute(con, "PRAGMA threads=4")
    DBI::dbExecute(con, "PRAGMA memory_limit='6GB'")
    DBI::dbExecute(con, sprintf(
      "PRAGMA temp_directory='%s'",
      file.path(tempdir(), "ppduck")
    ))

    TP <- DBI::dbQuoteString(con, path.expand(PP_TP_GLOB))
    SP <- DBI::dbQuoteString(con, path.expand(PP_SP_GLOB))

    # 2a) Conversion-Ebene = Produkt-Ebene. gross_revenue per MAX (repliziert!).
    message("  [1/4] Conversions ...")
    DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE TEMP TABLE conv AS
    SELECT conversion_id,
           ANY_VALUE(product_title)      AS product_title,
           MAX(gross_revenue)            AS g,
           MAX(net_revenue)              AS net,
           MAX(cm2)                      AS cm2,
           MAX(gross_sold_quantity)      AS qty
    FROM read_parquet(%s)
    WHERE shopify_order_id IS NOT NULL AND product_title IS NOT NULL
    GROUP BY conversion_id
    HAVING MAX(gross_revenue) > 0", TP))

    # 2b) Touchpoints je conversion x Klicktag x Kanal x Kampagne, Gewicht auf 1
    #     normiert. NULL-Kampagne bekommt ein Sentinel, damit sie joinbar bleibt.
    message("  [2/4] Touchpoints & Gewichte ...")
    DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE TEMP TABLE tpw AS
    WITH roh AS (
      SELECT conversion_id,
             CAST(channel_date AS DATE)                    AS klicktag,
             COALESCE(channel_group, '(ohne Kanal)')       AS kanal,
             COALESCE(channel_campaign, '(ohne Kampagne)') AS kampagne,
             SUM(%s) AS w
      FROM read_parquet(%s)
      WHERE shopify_order_id IS NOT NULL AND product_title IS NOT NULL
        AND channel_date IS NOT NULL
      GROUP BY 1,2,3,4
    )
    SELECT conversion_id, klicktag, kanal, kampagne,
           w / NULLIF(SUM(w) OVER (PARTITION BY conversion_id), 0) AS nw
    FROM roh", PP_MODELL, TP))

    # 2c) Attribuierter Umsatz je Klicktag x Kanal x Kampagne x Familie
    message("  [3/4] Attribuierter Umsatz je Familie ...")
    DBI::dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE ar AS
    SELECT t.klicktag, t.kanal, t.kampagne,
           c.product_title,
           SUM(t.nw * c.g)   AS ar_gross,
           SUM(t.nw * c.net) AS ar_net,
           SUM(t.nw * c.cm2) AS ar_cm2,
           SUM(t.nw * c.qty) AS ar_qty,
           SUM(t.nw)         AS ar_orders
    FROM tpw t
    JOIN conv c USING (conversion_id)
    WHERE t.nw IS NOT NULL
    GROUP BY 1,2,3,4
    HAVING SUM(t.nw * c.g) > 0")

    # Kontrolle: Attribution darf keinen Umsatz verlieren oder erfinden.
    k <- DBI::dbGetQuery(con, "
    SELECT (SELECT SUM(g) FROM conv WHERE conversion_id IN (SELECT conversion_id FROM tpw)) AS conv_gross,
           (SELECT SUM(ar_gross) FROM ar) AS attribuiert")
    abw <- abs(k$conv_gross - k$attribuiert) / k$conv_gross
    if (is.na(abw) || abw > 1e-6) {
      stop(
        "Attributions-Kontrolle fehlgeschlagen: Conversion-Umsatz ",
        round(k$conv_gross), " vs. attribuiert ", round(k$attribuiert)
      )
    }
    message(
      "      Kontrolle ok: ", formatC(round(k$attribuiert), format = "d", big.mark = ".", decimal.mark = ","),
      " EUR attribuiert, Abweichung ", signif(abw, 2)
    )

    # 2d) Spend allozieren -- zweistufig, Rest bleibt ausgewiesen.
    message("  [4/4] Spend-Allokation ...")
    DBI::dbExecute(con, sprintf("
    CREATE OR REPLACE TEMP TABLE sp AS
    SELECT CAST(date AS DATE)                           AS klicktag,
           COALESCE(channel_group, '(ohne Kanal)')      AS kanal,
           COALESCE(channel_campaign, '(ohne Kampagne)') AS kampagne,
           SUM(COALESCE(channel_spend, 0))              AS spend
    FROM read_parquet(%s)
    GROUP BY 1,2,3
    HAVING SUM(COALESCE(channel_spend, 0)) > 0", SP))

    # Fenster: Touchpoints und Spend enden nicht am selben Tag. Spend nach dem
    # letzten Klicktag der Touchpoints hat strukturell keinen Umsatzpartner und
    # darf NICHT auf Produkte verteilt werden -- er wird separat ausgewiesen.
    datenende <- DBI::dbGetQuery(con, "SELECT MAX(klicktag) AS d FROM ar")$d
    message("      Datenende Touchpoints: ", datenende)

    DBI::dbExecute(con, "
    CREATE OR REPLACE TEMP TABLE alloc AS
    WITH
    -- Stufe 1: kampagnengenau. Traegt, weil die Kampagnen produktspezifisch
    -- benannt sind.
    s1 AS (
      SELECT sp.klicktag, sp.kanal, a.product_title,
             sp.spend * a.ar_gross
               / SUM(a.ar_gross) OVER (PARTITION BY sp.klicktag, sp.kanal, sp.kampagne)
               AS spend_alloc,
             1 AS stufe
      FROM sp JOIN ar a USING (klicktag, kanal, kampagne)
    ),
    -- Spend ohne Kampagnen-Partner. Dazu gehoert auch Adtributes Sammelkanal
    -- Catch All, dem kaum Conversions zugeordnet sind.
    rest AS (
      SELECT sp.klicktag, SUM(sp.spend) AS spend
      FROM sp
      LEFT JOIN (SELECT DISTINCT klicktag, kanal, kampagne FROM ar) a
        USING (klicktag, kanal, kampagne)
      WHERE a.kampagne IS NULL
      GROUP BY 1
    ),
    -- Tages-Mix ueber ALLE Kanaele als Verteilschluessel.
    ar_tag AS (
      SELECT klicktag, product_title, SUM(ar_gross) AS ar_gross
      FROM ar GROUP BY 1,2
    ),
    -- Stufe 2: bewusst NICHT der Eigen-Mix des jeweiligen Kanals. Ohne
    -- Kampagnen-Treffer gibt es keine produktspezifische Information; der
    -- annahmeaermste Schluessel ist der Umsatzanteil des Produkts am Tag.
    s2 AS (
      SELECT r.klicktag, '(ohne Kanalbezug)' AS kanal, t.product_title,
             r.spend * t.ar_gross
               / SUM(t.ar_gross) OVER (PARTITION BY r.klicktag)
               AS spend_alloc,
             2 AS stufe
      FROM rest r JOIN ar_tag t USING (klicktag)
    )
    SELECT * FROM s1 UNION ALL SELECT * FROM s2")

    stufen <- DBI::dbGetQuery(
      con,
      "SELECT stufe, SUM(spend_alloc) spend FROM alloc GROUP BY 1 ORDER BY 1"
    )
    for (i in seq_len(nrow(stufen))) {
      message(
        "      Stufe ", stufen$stufe[i], ": ",
        formatC(round(stufen$spend[i]), format = "d", big.mark = ".", decimal.mark = ","), " EUR"
      )
    }

    # Nicht zugeordnet = Tage ohne jeden attribuierten Umsatz. Getrennt danach,
    # ob sie hinter dem Datenende der Touchpoints liegen (Sync-Rueckstand) oder
    # innerhalb des Fensters (echte Luecke).
    spend_offen <- DBI::dbGetQuery(con, sprintf("
    SELECT r.klicktag, r.spend,
           CASE WHEN r.klicktag > DATE '%s' THEN 'nach Datenende'
                ELSE 'ohne Attribution' END AS grund
    FROM (SELECT sp.klicktag, SUM(sp.spend) AS spend
          FROM sp LEFT JOIN (SELECT DISTINCT klicktag, kanal, kampagne FROM ar) a
            USING (klicktag, kanal, kampagne)
          WHERE a.kampagne IS NULL GROUP BY 1) r
    LEFT JOIN (SELECT DISTINCT klicktag FROM ar) k USING (klicktag)
    WHERE k.klicktag IS NULL", datenende))

    marketing <- DBI::dbGetQuery(con, "
    SELECT COALESCE(a.klicktag, s.klicktag)       AS klicktag,
           COALESCE(a.kanal, s.kanal)             AS kanal,
           COALESCE(a.product_title, s.product_title) AS product_title,
           COALESCE(a.ar_gross, 0)   AS ar_gross,
           COALESCE(a.ar_net, 0)     AS ar_net,
           COALESCE(a.ar_cm2, 0)     AS ar_cm2,
           COALESCE(a.ar_qty, 0)     AS ar_qty,
           COALESCE(a.ar_orders, 0)  AS ar_orders,
           COALESCE(s.spend_alloc, 0) AS spend_alloc
    FROM (SELECT klicktag, kanal, product_title, SUM(ar_gross) ar_gross,
                 SUM(ar_net) ar_net, SUM(ar_cm2) ar_cm2, SUM(ar_qty) ar_qty,
                 SUM(ar_orders) ar_orders
          FROM ar GROUP BY 1,2,3) a
    FULL OUTER JOIN (SELECT klicktag, kanal, product_title, SUM(spend_alloc) spend_alloc
                     FROM alloc GROUP BY 1,2,3) s
      USING (klicktag, kanal, product_title)")

    spend_gesamt <- DBI::dbGetQuery(con, "SELECT SUM(spend) s FROM sp")$s
    list(
      marketing = marketing, spend_offen = spend_offen,
      spend_gesamt = spend_gesamt, datenende = as.Date(datenende)
    )
  },
  finally = DBI::dbDisconnect(con, shutdown = TRUE)
)

marketing <- ergebnis$marketing
spend_offen <- ergebnis$spend_offen

marketing$klicktag <- as.Date(marketing$klicktag)
spend_offen$klicktag <- as.Date(spend_offen$klicktag)

zugeordnet <- sum(marketing$spend_alloc, na.rm = TRUE)
offen <- sum(spend_offen$spend, na.rm = TRUE)
abdeckung <- zugeordnet / ergebnis$spend_gesamt

message("  Spend gesamt      : ", formatC(round(ergebnis$spend_gesamt), format = "d", big.mark = ".", decimal.mark = ","), " EUR")
message(
  "  davon zugeordnet  : ", formatC(round(zugeordnet), format = "d", big.mark = ".", decimal.mark = ","), " EUR (",
  round(100 * abdeckung, 1), " %)"
)
message("  nicht zuordenbar  : ", formatC(round(offen), format = "d", big.mark = ".", decimal.mark = ","), " EUR")

# Grobe Plausibilitaet: Allokation darf den Spend nicht vermehren.
if (zugeordnet > ergebnis$spend_gesamt * 1.0001) {
  stop("Spend-Allokation groesser als gebuchter Spend -- Allokation pruefen.")
}

bundle <- list(
  familien = familien,
  marketing = marketing,
  spend_offen = spend_offen,
  meta = list(
    generated_at = Sys.time(),
    modell = PP_MODELL,
    zeitbasis = "Klickdatum (channel_date)",
    von = min(marketing$klicktag, na.rm = TRUE),
    bis = max(marketing$klicktag, na.rm = TRUE),
    spend_gesamt = ergebnis$spend_gesamt,
    spend_alloziert = zugeordnet,
    spend_offen = offen,
    spend_offen_nach_datenende = sum(spend_offen$spend[spend_offen$grund == "nach Datenende"]),
    spend_abdeckung = abdeckung,
    datenende = ergebnis$datenende,
    n_familien = dplyr::n_distinct(familien$produkt_key),
    n_titel_offen = n_offen
  )
)

saveRDS(bundle, path.expand(PP_OUT), compress = "xz")
message(
  "[", Sys.time(), "] Produktperformance-Bundle geschrieben: ", PP_OUT,
  " (", round(file.size(path.expand(PP_OUT)) / 1024^2, 2), " MB, ",
  formatC(nrow(marketing), format = "d", big.mark = ".", decimal.mark = ","), " Zeilen)"
)
