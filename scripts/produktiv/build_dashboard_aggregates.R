# ============================================================================
# build_dashboard_aggregates.R (MINIMAL VERSION)
# ============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(DBI)
  library(duckdb)
})

DASH_DATA <- "~/git/dashboard/data"
message("[", Sys.time(), "] Verbinde mit DuckDB für Produkt-Performance...")

# 1. Lokale Daten laden
returns <- readRDS(file.path(DASH_DATA, "all_returns_cleaned.rds")) |>
  mutate(shopify_order_id = as.numeric(shopify_order_id))

# 2. Verbindung zur DuckDB -- READ_ONLY: der Serving-Mart wird NICHT geschrieben
#    (verhindert Locks/haengende Verbindungen, die Folge-Schritte/Re-Runs kaputt
#    machen). Kein on.exit() (bekannter Bug beim Sourcen) -> tryCatch/finally.
con <- DBI::dbConnect(duckdb::duckdb(),
  dbdir = file.path(DASH_DATA, "shopify.duckdb"),
  read_only = TRUE
)

tryCatch({
  # 3. Retouren als VIRTUELLE Tabelle registrieren (kein Schreibzugriff noetig)
  returns_for_join <- returns |>
    filter(type == "return") |>
    select(shopify_order_id, type) |>
    distinct()
  duckdb::duckdb_register(con, "returns_temp", returns_for_join)

  shopify_db <- dplyr::tbl(con, "orders")
  returns_db <- dplyr::tbl(con, "returns_temp")

  # 4. Aggregation in der Datenbank
  shopifys_without_returns <- shopify_db |>
    select(
      order_id, financial_status, item_gross_revenue, quantity,
      product_title, product_title_with_variant, variant_title
    ) |>
    rename(shopify_order_id = order_id) |>
    left_join(returns_db, by = "shopify_order_id") |>
    filter(is.na(type)) |>
    filter(financial_status %in% c("paid", "partially_paid")) |>
    select(-type) |>
    collect() |> # 🚀 Ab hier sind wir sicher im RAM!
    mutate(
      product_title_with_variant = str_replace_all(product_title_with_variant, "Pammys™ - ", ""),
      product_title_with_variant = str_replace_all(product_title_with_variant, "PillowSteps ", ""),
      product_title_with_variant = str_replace_all(product_title_with_variant, "Jahressale ", ""),
      product_title_with_variant = str_remove(product_title_with_variant, "/.*$") |> str_trim()
    )

  product_choices_ranked <- shopifys_without_returns |>
    filter(!is.na(product_title) & product_title != "") |>
    group_by(product_title) |>
    summarise(total_sold = sum(quantity, na.rm = TRUE), .groups = "drop") |>
    arrange(desc(total_sold)) |>
    pull(product_title)

  # 5. Speichern
  saveRDS(shopifys_without_returns, file.path(DASH_DATA, "shopifys_without_returns.rds"))
  saveRDS(product_choices_ranked, file.path(DASH_DATA, "product_choices_ranked.rds"))
}, finally = {
  # Verbindung IMMER schliessen -> keine haengenden Locks fuer Folge-Schritte
  try(duckdb::duckdb_unregister(con, "returns_temp"), silent = TRUE)
  try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
})

# ============================================================================
# 6. Bearbeiter-Zuordnung Rueckgaben <-> Zendesk  (NEU 14.08.2026)
# ============================================================================
# Das Retourenportal protokolliert NICHT, wer einen Vorgang bearbeitet hat.
# Ueber die Zendesk-Tickets laesst sich aber zumindest ermitteln, wer den
# SUPPORT-KONTAKT zur selben Bestellung hatte. Join-Schluessel ist die
# 9-stellige Shopify-Bestellnummer (returns$order_number ohne "#" <->
# tickets$bestellnummer).
#
# ACHTUNG, Vorgeschichte: bis zum 14.08.2026 hat clean_up_zendesk() die
# Bestellnummer per Regex auf 8 Stellen abgeschnitten (Shopify ist laengst
# 9-stellig). Dadurch matchten von 438.661 Retouren nur 354 -- der Join sah
# unmoeglich aus, war aber bloss kaputt. Wenn diese Datei ploetzlich leer
# ist, ist als Erstes zu pruefen, ob eine alte dieseoR-Version installiert
# ist: `table(nchar(tickets$bestellnummer))` muss 9 liefern, nicht 8.
message("[", Sys.time(), "] Baue Bearbeiter-Zuordnung (Retouren x Zendesk)...")

returns_agent_map <- tryCatch(
  {
    tickets <- readRDS(file.path(DASH_DATA, "all_tickets_selected.rds"))

    if (!"bestellnummer" %in% names(tickets)) {
      stop("Spalte 'bestellnummer' fehlt in all_tickets_selected.rds")
    }
    laengen <- table(nchar(tickets$bestellnummer))
    if (!"9" %in% names(laengen)) {
      warning(
        "Keine 9-stelligen Bestellnummern in den Tickets -- ",
        "vermutlich laeuft noch die alte clean_up_zendesk()-Version. ",
        "Die Bearbeiter-Zuordnung bleibt dann leer."
      )
    }

    # Nur echte Personen. "AI" ist der Bot (bearbeitet mit 137k Tickets rund
    # die Haelfte allein und ist der Grund fuer die begrenzte Abdeckung),
    # "pammys_support" ein Sammelaccount, rein numerische Werte sind Agenten
    # ohne Eintrag in der id_name_map.
    keine_person <- c("AI", "pammys_support")
    tickets |>
      filter(
        !is.na(bestellnummer), !is.na(assignee_id),
        !assignee_id %in% keine_person,
        !str_detect(assignee_id, "^[0-9]+$"),
        # Plausibilitaet: echte Shopify-Bestellnummern beginnen mit "12"
        # (121xxxxxx-122xxxxxx). Der Fallback \\b\\d{8}\\b im Extraktions-
        # Regex faengt sonst auch Hausnummern, Datumsketten o. ae. ein
        # ("00000000", "04250236") und wuerde Falsch-Joins erzeugen.
        str_detect(bestellnummer, "^12[0-9]{6,7}$")
      ) |>
      # Regel bei mehreren Tickets zur selben Bestellung: der ZULETZT
      # eroeffnete Kontakt gewinnt -- das ist der Stand, der zum Vorgang passt.
      arrange(desc(created_at)) |>
      group_by(bestellnummer) |>
      summarise(
        bearbeiter = first(assignee_id),
        tickets_n = n(),
        bearbeiter_n = n_distinct(assignee_id), # >1 = Vorgang wanderte
        .groups = "drop"
      )
  },
  error = function(e) {
    message("FEHLER bei der Bearbeiter-Zuordnung: ", conditionMessage(e))
    tibble::tibble(
      bestellnummer = character(), bearbeiter = character(),
      tickets_n = integer(), bearbeiter_n = integer()
    )
  }
)

saveRDS(returns_agent_map, file.path(DASH_DATA, "returns_agent_map.rds"))

abdeckung <- {
  onc <- str_remove(returns$order_number, "^#")
  round(100 * mean(onc %in% returns_agent_map$bestellnummer), 1)
}
message(
  "[", Sys.time(), "] returns_agent_map.rds: ",
  nrow(returns_agent_map), " Bestellungen, ",
  abdeckung, " % der Vorgaenge zuordenbar."
)

# ============================================================================
# 7. Produkt- und Kostenkennzahlen zu Retouren  (NEU 10.09.2026)
# ============================================================================
# Drei Vorberechnungen fuer den Tab "Produkte & Kosten". Alle drei aggregieren
# in DuckDB bzw. auf Order-Ebene und liefern kleine Dateien -- das Dashboard
# soll nichts davon live rechnen (2,5 Mio. Zeilen je Jahr).
message("[", Sys.time(), "] Baue Produkt- und Kostenkennzahlen ...")

VON <- "2025-01-01" # Betrachtungsfenster; aelter lohnt sich fachlich nicht

con2 <- DBI::dbConnect(duckdb::duckdb(),
  dbdir = file.path(DASH_DATA, "shopify.duckdb"),
  read_only = TRUE
)

tryCatch({
  # --- 7a) Retourenquote je SKU -------------------------------------------
  # Bezugsgroesse ist die STUECKZAHL, nicht der Umsatz: eine Quote von
  # "13 % der Stueck" ist die Zahl, die Einkauf und Produktmanagement
  # brauchen. Der Wert steht daneben, damit man teure von billigen
  # Retouren unterscheiden kann.
  sku_returns <- DBI::dbGetQuery(con2, sprintf("
    SELECT
      product_sku,
      any_value(product_title)              AS produkt,
      any_value(variant_title)              AS variante,
      date_trunc('month', created_at)       AS monat,
      sum(quantity)                         AS verkauft,
      sum(returned_quantity)                AS retourniert,
      sum(item_gross_revenue)               AS brutto_umsatz,
      sum(returned_amount)                  AS retour_wert
    FROM orders
    WHERE created_at >= TIMESTAMP '%s'
      AND product_sku IS NOT NULL AND product_sku <> ''
    GROUP BY product_sku, date_trunc('month', created_at)", VON))
  saveRDS(sku_returns, file.path(DASH_DATA, "sku_returns.rds"))
  message(
    "   sku_returns.rds: ", nrow(sku_returns), " SKU-Monats-Zeilen, ",
    length(unique(sku_returns$product_sku)), " SKUs"
  )

  # --- 7b) Voll- vs. Teilretoure je Order ---------------------------------
  # "100 % Rueckgabe" heisst: jedes bestellte Stueck kam zurueck. Das laesst
  # sich nur auf Order-Ebene entscheiden, deshalb erst je Order verdichten
  # und dann je Monat zaehlen.
  order_return_profile <- DBI::dbGetQuery(con2, sprintf("
    WITH o AS (
      SELECT order_id,
             date_trunc('month', min(created_at)) AS monat,
             sum(quantity)          AS q,
             sum(returned_quantity) AS r,
             sum(item_gross_revenue) AS umsatz,
             sum(returned_amount)    AS retour_wert
      FROM orders WHERE created_at >= TIMESTAMP '%s'
      GROUP BY order_id
    )
    SELECT monat,
           count(*)                                        AS orders,
           count(*) FILTER (WHERE r = 0)                    AS ohne_retoure,
           count(*) FILTER (WHERE r > 0 AND r < q)          AS teil_retoure,
           count(*) FILTER (WHERE r >= q AND r > 0)         AS voll_retoure,
           round(sum(umsatz), 2)                            AS umsatz,
           round(sum(retour_wert) FILTER (WHERE r >= q AND r > 0), 2) AS wert_voll,
           round(sum(retour_wert) FILTER (WHERE r > 0 AND r < q), 2)  AS wert_teil
    FROM o GROUP BY monat ORDER BY monat", VON))
  saveRDS(order_return_profile, file.path(DASH_DATA, "order_return_profile.rds"))
  message("   order_return_profile.rds: ", nrow(order_return_profile), " Monate")

  # --- 7c) Kosten der Umtausche -------------------------------------------
  # Ein Umtausch erzeugt in Shopify eine ERSATZBESTELLUNG. Deren Warenwert
  # geht raus, ohne dass Geld hereinkommt -- das ist der eigentliche Preis
  # eines Umtauschs und steht in keiner Erstattungssumme.
  #
  # Der Link dorthin steckt in `shopify_new_order_path` der Rohdaten
  # (Beispiel: .../orders/6886956237129). clean_up_returns() wirft das Feld
  # als PII-Kandidat weg, deshalb hier die Rohdatei lesen.
  # Abdeckung (Messung 10.09.2026): 85,4 % der Umtausche, davon 99,99 %
  # in Shopify auffindbar.
  exchange_costs <- tryCatch(
    {
      raw_ret <- readRDS(file.path(datadir_returns <- "~/data/returns", "all_returns.rds"))
      er <- raw_ret |>
        mutate(
          del      = !is.na(deleted_at) & deleted_at != "",
          neu_id   = suppressWarnings(as.numeric(str_extract(shopify_new_order_path, "[0-9]+$"))),
          monat    = as.Date(format(as.POSIXct(created_at, tz = "UTC"), "%Y-%m-01"))
        ) |>
        filter(!del, type %in% c("exchange", "mix"), !is.na(neu_id)) |>
        # In der ROHdatei sind die Betragsfelder noch Text -- die Umwandlung
        # macht sonst clean_up_returns(), das wir hier bewusst umgehen.
        mutate(shipping_cost = suppressWarnings(as.numeric(shipping_cost))) |>
        select(monat, type, neu_id, shipping_cost)

      duckdb::duckdb_register(con2, "ersatz_ref", er |> distinct(neu_id))
      kosten <- DBI::dbGetQuery(con2, "
      SELECT o.order_id AS neu_id,
             sum(o.item_gross_revenue) AS warenwert,
             any_value(o.total_price)  AS total_price
      FROM orders o JOIN ersatz_ref e ON o.order_id = e.neu_id
      GROUP BY o.order_id")
      duckdb::duckdb_unregister(con2, "ersatz_ref")

      er |>
        left_join(kosten, by = "neu_id") |>
        group_by(monat, type) |>
        summarise(
          vorgaenge = n(),
          mit_ersatzorder = sum(!is.na(warenwert)),
          warenwert_raus = round(sum(warenwert, na.rm = TRUE), 2),
          gegenwert = round(sum(total_price, na.rm = TRUE), 2),
          retour_versand = round(sum(shipping_cost, na.rm = TRUE), 2),
          .groups = "drop"
        )
    },
    error = function(e) {
      message("   FEHLER bei den Umtausch-Kosten: ", conditionMessage(e))
      NULL
    }
  )
  if (!is.null(exchange_costs)) {
    saveRDS(exchange_costs, file.path(DASH_DATA, "exchange_costs.rds"))
    message(
      "   exchange_costs.rds: ", nrow(exchange_costs), " Monats-Zeilen, ",
      "Warenwert gesamt ", round(sum(exchange_costs$warenwert_raus)), " EUR"
    )
  }
}, finally = {
  try(DBI::dbDisconnect(con2, shutdown = TRUE), silent = TRUE)
})

message("[", Sys.time(), "] Fertig. Aggregate erstellt: shopifys_without_returns, product_choices_ranked, returns_agent_map, sku_returns, order_return_profile, exchange_costs.")
