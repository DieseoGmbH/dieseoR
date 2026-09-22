# =============================================================================
# Skript: ~/git/dieseoR/scripts/produktiv/sync_adtribute_delta.R
# Inkrementelles Laden der Adtribute-Daten (Snapshot-Diff nach Adtribute-Muster,
# hash-optimiert, da der Pammys-SA in pammys-analytics keine Tabellen anlegen
# darf -> Sync-Tabellen liegen im eigenen Projekt data-analytics-491117).
#
# Aufruf:
#   Rscript sync_adtribute_delta.R bootstrap   # Einmalig: Snapshot + kompletter
#                                              # konsistenter Neu-Pull der Chunks
#   Rscript sync_adtribute_delta.R delta       # Nächtlich: nur Änderungen ziehen
#
# Design (warum nicht 1:1 der Adtribute-Beispielcode):
#   * int_attribution ist ein VIEW (281 GB Scan, unpartitioniert). Der Original-
#     Ansatz (voller Spalten-Snapshot + EXCEPT DISTINCT) würde ~280 GB Storage
#     duplizieren und ~4 Full-Scans/Nacht kosten.
#   * Stattdessen: Snapshot = (conversion_id, touchpoint_id, channel_month,
#     row_hash) mit row_hash = FARM_FINGERPRINT(TO_JSON_STRING(<92 Spalten>)).
#     WICHTIG: (conversion_id, touchpoint_id) ist NICHT eindeutig (~13k echte
#     Quell-Dubletten mit unterschiedlichem Inhalt). Deshalb Set-Semantik:
#     Identität einer Zeile = (conversion_id, touchpoint_id, row_hash);
#     eine Änderung erscheint als deleted(alter Hash) + upserted(neuer Hash).
#     Die Chunks tragen row_hash als zusätzliche Spalte, damit das lokale
#     Löschen exakt die richtige Zeilen-Version trifft.
#     Delta = FULL OUTER JOIN View vs. Snapshot -> 'upserted' (neu ODER geändert,
#     inkl. voller Datenspalten) und 'deleted' (nur noch im Snapshot).
#   * Snapshot-Update per MERGE aus dem Delta (kein 2. Full-Scan) und ERST
#     NACHDEM das lokale Einspielen erfolgreich war -> ein Crash dazwischen ist
#     harmlos, der nächste Lauf berechnet dasselbe Delta erneut (idempotent).
#   * Lokal: DuckDB schreibt nur die betroffenen Monats-Chunks neu
#     (Anti-Join auf die Delta-Keys + Insert der upserted-Zeilen, atomarer Swap).
#
# Kosten je Delta-Lauf: 1 Full-Scan des Views (~281 GB ≈ 1,76 USD) + Kleinkram.
# =============================================================================

suppressMessages({
  library(bigrquery)
  library(arrow)
  library(dplyr)
  library(DBI)
  library(duckdb)
})

if (file.exists("~/workspace/local.R")) {
  source("~/workspace/local.R")
} else {
  stop("local.R nicht gefunden! Bitte Pfad prüfen.")
}

# --- Konfiguration ------------------------------------------------------------
MODE <- commandArgs(trailingOnly = TRUE)[1]
if (is.na(MODE)) MODE <- "delta"
stopifnot(MODE %in% c("bootstrap", "delta", "repair", "serving"))

JSON_KEY <- "~/git/dieseoR/scripts/auth_keys/pammys-analytics-bac507b00184.json"
BILLING <- "pammys-analytics" # Job läuft hier (SA hat jobUser)
SRC <- "`pammys-analytics.adtribute_raw.int_attribution_cmp10cxdk0028wgrcaicb16dp`"
SYNC <- "data-analytics-491117.adtribute_sync" # eigenes Dataset (EU), SA ist WRITER
T_STATE <- sprintf("`%s.int_attribution_sync_state`", SYNC)
T_DELTA <- sprintf("`%s.int_attribution_delta`", SYNC)
T_BASE <- sprintf("`%s.int_attribution_baseline`", SYNC)
T_SERVING <- sprintf("`%s.int_attribution_serving`", SYNC)
START_DATE <- "2024-11-01"
MAX_DELTA_ROWS <- 20e6 # Notbremse: darüber ist ein bootstrap sinnvoller

# --- Serving-Tabelle für die Online-Live-Drilldowns (Shiny auf shinyapps.io) ---
# Schlanke Spiegelung (nur die 19 Spalten der Live-Tabs) der lokalen Chunks,
# partitioniert nach channel_date + geclustert nach shopify_order_id -> Online-
# Queries kosten Centbruchteile. Wird nächtlich per MERGE aus dem Delta gepflegt
# (gleiche Zeilen-Identität conversion_id+touchpoint_id+row_hash, ~0,4 $/Nacht).
SERVING_COLS <- c(
  "conversion_id", "touchpoint_id", "row_hash", "channel_date",
  "channel_hour", "touchpoint_attribution_datetime", "conversion_attribution_datetime",
  "channel_group", "channel_name", "channel_campaign", "landingpage",
  "sessions", "shopify_order_id", "gross_revenue",
  "attribution_weight_last", "weight_pammys_opt", "customer_segment",
  "product_title"
)
# SELECT-Liste: channel_date als echtes DATE (Partitionsspalte)
serving_select <- paste(
  ifelse(SERVING_COLS == "channel_date",
    "CAST(channel_date AS DATE) AS channel_date", SERVING_COLS
  ),
  collapse = ", "
)

CHUNK_DIR <- file.path(path.expand(datadir), "adtribute_parquet_chunks_full")
BOOT_DIR <- paste0(CHUNK_DIR, "_new") # bootstrap zieht non-destruktiv hierhin
STAGING_DIR <- file.path(path.expand(datadir), "adtribute_delta_staging")
for (d in c(STAGING_DIR)) if (!dir.exists(d)) dir.create(d, recursive = TRUE)

# --- SELECT-Block: MUSS 1:1 dem Schema der bestehenden Chunks entsprechen -----
# (Basis: get_adtribute_raw_data_parquet.R; Schema-Änderungen bei Adtribute
#  seit 2026-06: pammys_optimized -> pammys_optimized_3 (Modell neu versioniert);
#  u_shape_non_brand___30d wurde GELÖSCHT -> als NULL gehalten, damit das
#  Chunk-Schema stabil bleibt. Bei erneuten Schema-Fehlern: View-Schema prüfen!)
select_block <- "
  conversion_id,
  conversion_key,
  touchpoint_id,
  touchpoint_key,
  conversion_attribution_datetime,
  touchpoint_attribution_datetime,
  attribution_journey_dynamic_first_touchpoint_datetime AS first_touchpoint_datetime,
  channel_date,
  channel_hour,
  attribution_touchpoint_index_last,
  attribution_touchpoint_index_non_direct_first,
  attribution_touchpoint_index_non_direct_last,
  channel_name,
  channel_campaign,
  channel_adset,
  channel_ad,
  channel_id,
  attributes.custom_channel_attribute_channel_group  AS channel_group,
  attributes.custom_channel_attribute_bucket         AS bucket,
  attributes.custom_channel_attribute_marketplace    AS marketplace,
  attributes.custom_channel_attribute_meta_ads_account_id  AS meta_ads_account_id,
  attributes.custom_channel_attribute_meta_ads_campaign_id AS meta_ads_campaign_id,
  attributes.custom_channel_attribute_meta_ads_adset_id    AS meta_ads_adset_id,
  attributes.custom_channel_attribute_meta_ads_ad_id       AS meta_ads_ad_id,
  attributes.custom_channel_attribute_meta_ads_creative_type AS meta_ads_creative_type,
  attribution_weight_last,
  attribution_weight_non_direct_first,
  attribution_weight_non_direct_last,
  attribution_weight_non_direct_linear AS weight_linear,
  attribution_weight_custom_attribution_model_pammys_optimized_3      AS weight_pammys_opt,
  CAST(NULL AS FLOAT64)                                                AS weight_u_shape_30d,
  attribution_weight_custom_attribution_model_non_direct_first___1d    AS weight_first_1d,
  attribution_weight_custom_attribution_model_non_direct_first___7d    AS weight_first_7d,
  attribution_weight_custom_attribution_model_non_direct_first___30d   AS weight_first_30d,
  attribution_weight_custom_attribution_model_non_direct_full_impact___7d  AS weight_full_impact_7d,
  attribution_weight_custom_attribution_model_non_direct_full_impact___30d AS weight_full_impact_30d,
  attribution_weight_custom_attribution_model_non_direct_linear___7d   AS weight_linear_7d,
  attributes.custom_conversion_attribute_shopify_order_id AS shopify_order_id,
  attributes.custom_conversion_attribute_gross_revenue    AS gross_revenue,
  attributes.custom_conversion_attribute_net_revenue      AS net_revenue,
  attributes.custom_conversion_attribute_gmv              AS gmv,
  attributes.custom_conversion_attribute_gross_taxes      AS gross_taxes,
  attributes.custom_conversion_attribute_taxes            AS taxes,
  attributes.custom_conversion_attribute_refunded_taxes   AS refunded_taxes,
  attributes.custom_conversion_attribute_refunds          AS refunds,
  attributes.custom_conversion_attribute_discounts        AS discounts,
  attributes.custom_conversion_attribute_shipping_revenue AS shipping_revenue,
  attributes.custom_conversion_attribute_product_costs    AS product_costs,
  attributes.custom_conversion_attribute_transaction_costs AS transaction_costs,
  attributes.custom_conversion_attribute_fulfillment_costs AS fulfillment_costs,
  attributes.custom_conversion_attribute_cm1              AS cm1,
  attributes.custom_conversion_attribute_cm2              AS cm2,
  attributes.custom_conversion_attribute_cumulative_cm2   AS cumulative_cm2,
  attributes.custom_conversion_attribute_gross_orders     AS gross_orders,
  attributes.custom_conversion_attribute_net_orders       AS net_orders,
  attributes.custom_conversion_attribute_refunded_orders  AS refunded_orders,
  attributes.custom_conversion_attribute_gross_sold_quantity AS gross_sold_quantity,
  attributes.custom_conversion_attribute_nc_gross_revenue      AS nc_gross_revenue,
  attributes.custom_conversion_attribute_nc_net_revenue        AS nc_net_revenue,
  attributes.custom_conversion_attribute_nc_cm2                AS nc_cm2,
  attributes.custom_conversion_attribute_nc_gross_orders       AS nc_gross_orders,
  attributes.custom_conversion_attribute_nc_net_orders         AS nc_net_orders,
  attributes.custom_conversion_attribute_nc_refunded_orders    AS nc_refunded_orders,
  attributes.custom_conversion_attribute_nc_gross_sold_quantity AS nc_gross_sold_quantity,
  attributes.custom_conversion_attribute_rc_gross_revenue      AS rc_gross_revenue,
  attributes.custom_conversion_attribute_rc_net_revenue        AS rc_net_revenue,
  attributes.custom_conversion_attribute_rc_cm2                AS rc_cm2,
  attributes.custom_conversion_attribute_rc_gross_orders       AS rc_gross_orders,
  attributes.custom_conversion_attribute_rc_net_orders         AS rc_net_orders,
  attributes.custom_conversion_attribute_rc_refunded_orders    AS rc_refunded_orders,
  attributes.custom_conversion_attribute_rc_gross_sold_quantity AS rc_gross_sold_quantity,
  attributes.custom_conversion_attribute_customer_segment   AS customer_segment,
  attributes.custom_conversion_attribute_identity           AS conversion_identity,
  attributes.custom_conversion_attribute_financial_status   AS financial_status,
  attributes.custom_conversion_attribute_fulfillment_status AS fulfillment_status,
  attributes.custom_conversion_attribute_cancellation_status AS cancellation_status,
  attributes.custom_conversion_attribute_shipping_country   AS shipping_country,
  attributes.custom_conversion_attribute_sales_channel      AS sales_channel,
  attributes.custom_conversion_attribute_payment_method     AS payment_method,
  attributes.custom_conversion_attribute_discount_code      AS discount_code,
  attributes.custom_conversion_attribute_discount_code_group AS discount_code_group,
  attributes.custom_conversion_attribute_klaviyo_list_name  AS klaviyo_list_name,
  attributes.custom_conversion_attribute_product_title      AS product_title,
  attributes.custom_conversion_attribute_product_title_with_variant AS product_title_with_variant,
  attributes.custom_conversion_attribute_product_title_with_color   AS product_title_with_color,
  attributes.custom_conversion_attribute_product_variant_title      AS product_variant_title,
  attributes.custom_conversion_attribute_product_sku        AS product_sku,
  attributes.custom_conversion_attribute_shopify_order_first_refund_datetime AS first_refund_datetime,
  attributes.custom_touchpoint_attribute_sessions     AS sessions,
  attributes.custom_touchpoint_attribute_nu_sessions  AS nu_sessions,
  attributes.custom_touchpoint_attribute_landinpage   AS landingpage,
  attributes.custom_touchpoint_attribute_touchpoint_name AS touchpoint_name
"

# CTE: aktueller View-Stand mit Monat + Zeilen-Hash (Hash über alle 92 Spalten;
# identische Doppel-Zeilen kollabieren zur Set-Semantik, inhaltlich
# unterschiedliche Dubletten je (conversion_id, touchpoint_id) bleiben erhalten).
cur_cte <- sprintf("
  WITH base AS (
    SELECT %s
    FROM %s
    WHERE channel_date >= '%s'
  ),
  cur AS (
    SELECT b.*,
           DATE_TRUNC(CAST(b.channel_date AS DATE), MONTH) AS channel_month,
           FARM_FINGERPRINT(TO_JSON_STRING(b))             AS row_hash
    FROM base b
  )", select_block, SRC, START_DATE)

# --- Helpers ------------------------------------------------------------------
run_query <- function(sql, what) {
  t0 <- Sys.time()
  message(sprintf("[bq] %s ...", what))
  tb <- bigrquery::bq_project_query(BILLING, sql, quiet = TRUE)
  message(sprintf(
    "[bq] %s fertig (%.1f min)", what,
    as.numeric(difftime(Sys.time(), t0, units = "mins"))
  ))
  invisible(tb)
}

# Download mit Smart Backoff (Muster aus get_adtribute_raw_data_parquet.R).
# bigint="integer64" ist PFLICHT: row_hash (FARM_FINGERPRINT) nutzt den vollen
# 64-bit-Bereich — der Default castet auf 32-bit-Integer -> Overflow -> lauter NA!
download_with_backoff <- function(tb, what) {
  page_sizes <- c(50000, 15000, 5000, 1000)
  for (i in seq_along(page_sizes)) {
    df <- tryCatch(
      bigrquery::bq_table_download(tb,
        page_size = page_sizes[i],
        bigint = "integer64", quiet = TRUE
      ),
      error = function(e) {
        warning(sprintf(
          "[dl] %s: Versuch %d (page_size %d) fehlgeschlagen: %s",
          what, i, page_sizes[i], conditionMessage(e)
        ))
        Sys.sleep(5 * i)
        NULL
      }
    )
    if (!is.null(df)) {
      return(df)
    }
  }
  stop("[dl] ", what, ": Download nach ", length(page_sizes), " Versuchen fehlgeschlagen.")
}

# Fail-Fast: Hash-Spalte muss vollständig sein, sonst ist das Delta-Apply blind.
assert_hashes_ok <- function(df, col, what) {
  if (col %in% names(df) && anyNA(df[[col]])) {
    stop(sprintf(
      "[guard] %s: %d NA in %s — Download-Typenproblem (bigint)?",
      what, sum(is.na(df[[col]])), col
    ))
  }
  invisible(df)
}

duck_con <- function() {
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = ":memory:")
  DBI::dbExecute(con, "PRAGMA threads=6")
  DBI::dbExecute(con, "PRAGMA memory_limit='10GB'")
  con
}

chunk_path <- function(dir, month) file.path(dir, sprintf("adtribute_raw_%s.parquet", format(month, "%Y_%m")))

state_exists <- function() {
  bigrquery::bq_table_exists(bigrquery::bq_table(
    "data-analytics-491117", "adtribute_sync",
    "int_attribution_sync_state"
  ))
}

# =============================================================================
# MODUS: bootstrap — konsistente Baseline (Snapshot + kompletter Chunk-Neu-Pull)
# =============================================================================
run_bootstrap <- function() {
  message("=== BOOTSTRAP: materialisiere Baseline (1 Full-Scan) ===")

  # RESUME-Logik: Wenn schon Chunks im BOOT_DIR liegen, MÜSSEN sie aus der noch
  # existierenden Baseline stammen — Baseline/State dann NICHT neu erzeugen,
  # sonst entsteht ein inkonsistenter Mix aus zwei Momentaufnahmen!
  boot_chunks <- length(list.files(BOOT_DIR, pattern = "parquet$"))
  base_exists <- bigrquery::bq_table_exists(
    bigrquery::bq_table("data-analytics-491117", "adtribute_sync", "int_attribution_baseline")
  )

  if (boot_chunks > 0 && !base_exists) {
    stop(
      "[boot] ", boot_chunks, " Chunks in ", BOOT_DIR, ", aber die Baseline-Tabelle ",
      "ist weg (Expiry?). Bitte BOOT_DIR löschen und Bootstrap KOMPLETT neu starten."
    )
  }

  if (boot_chunks > 0 && base_exists) {
    message(
      "[boot] RESUME: ", boot_chunks, " Chunks vorhanden, Baseline existiert noch — ",
      "überspringe Neuerstellung von Baseline & Sync-State."
    )
  } else {
    # 1) Baseline: EIN eingefrorener, partitionierter Stand des Views.
    #    Alle Monats-Pulls kommen danach aus DERSELBEN Momentaufnahme
    #    (konsistent + Partition-Pruning statt 21x Full-Scan).
    run_query(
      sprintf("
      CREATE OR REPLACE TABLE %s
      PARTITION BY channel_month
      OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 3 DAY))
      AS %s SELECT * FROM cur", T_BASE, cur_cte),
      "Baseline materialisieren"
    )

    # 2) Snapshot (Sync-State) = Keys + Monat + Hash aus der Baseline
    run_query(sprintf(
      "
      CREATE OR REPLACE TABLE %s AS
      SELECT conversion_id, touchpoint_id, channel_month, row_hash FROM %s",
      T_STATE, T_BASE
    ), "Sync-State aus Baseline ableiten")

    # 2b) Serving-Tabelle (Online-Live-Drilldowns) aus derselben Baseline
    #     (Spalten-Pruning: liest nur die 19 Serving-Spalten der Baseline)
    run_query(
      sprintf("
      CREATE OR REPLACE TABLE %s
      PARTITION BY channel_date
      CLUSTER BY shopify_order_id
      AS SELECT %s FROM %s", T_SERVING, serving_select, T_BASE),
      "Serving-Tabelle aus Baseline aufbauen"
    )
  }

  # 3) Monatsliste
  months <- run_query(
    sprintf(
      "SELECT DISTINCT channel_month FROM %s ORDER BY channel_month", T_BASE
    ),
    "Monatsliste"
  ) |>
    bigrquery::bq_table_download(quiet = TRUE) |>
    dplyr::pull(channel_month)
  message("[boot] Monate: ", paste(format(months, "%Y-%m"), collapse = ", "))

  # 4) Monats-Chunks aus der Baseline ziehen (checkpoint-fähig, non-destruktiv).
  #    row_hash bleibt in den Chunks (Teil der Zeilen-Identität fürs Delta-Apply).
  if (!dir.exists(BOOT_DIR)) dir.create(BOOT_DIR, recursive = TRUE)
  for (m in as.list(months)) {
    f <- chunk_path(BOOT_DIR, m)
    if (file.exists(f)) {
      message("[boot] ✓ ", basename(f), " existiert, überspringe.")
      next
    }
    tb <- run_query(
      sprintf(
        "SELECT * EXCEPT (channel_month) FROM %s WHERE channel_month = DATE '%s'",
        T_BASE, format(m, "%Y-%m-%d")
      ),
      paste("Monat", format(m, "%Y-%m"))
    )
    df <- download_with_backoff(tb, format(m, "%Y-%m"))
    assert_hashes_ok(df, "row_hash", format(m, "%Y-%m"))
    arrow::write_parquet(df, f)
    message(sprintf(
      "[boot] ✅ %s: %s Zeilen, %d Spalten",
      basename(f), format(nrow(df), big.mark = "."), ncol(df)
    ))
    rm(df)
    invisible(gc())
  }

  # 5) Vollständigkeit prüfen, dann atomarer Verzeichnis-Swap (alt -> Backup)
  missing <- months[!file.exists(vapply(as.list(months), function(m) chunk_path(BOOT_DIR, m), ""))]
  if (length(missing) > 0) {
    stop(
      "[boot] Unvollständig, KEIN Swap. Fehlende Monate: ",
      paste(format(missing, "%Y-%m"), collapse = ", "), " — Skript erneut starten (Checkpoints greifen)."
    )
  }

  backup <- paste0(CHUNK_DIR, "_backup_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  if (dir.exists(CHUNK_DIR)) {
    file.rename(CHUNK_DIR, backup)
    message("[boot] Alte Chunks -> ", backup)
  }
  file.rename(BOOT_DIR, CHUNK_DIR)
  message("[boot] Neue Chunks aktiv: ", CHUNK_DIR)

  # 6) Baseline aufräumen (hat zusätzlich 3-Tage-Expiry als Netz)
  run_query(sprintf("DROP TABLE IF EXISTS %s", T_BASE), "Baseline löschen")
  message("=== BOOTSTRAP FERTIG ===")
}

# =============================================================================
# Lokales Delta-Apply: schreibt je betroffenem Monat den Chunk neu
# (Anti-Join auf die Zeilen-Identität + Insert der upserted-Zeilen, atomarer
#  Datei-Swap). Idempotent: ein Wiederholungslauf desselben Deltas ändert nichts.
# Gibt die betroffenen Monate zurück.
# =============================================================================
apply_delta_locally <- function(staging, chunk_dir) {
  con <- duck_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  DBI::dbExecute(con, sprintf("CREATE VIEW delta AS SELECT * FROM read_parquet('%s')", staging))

  # Betroffene Monate: neue Position (channel_month) + alte Position (prev_month)
  months <- DBI::dbGetQuery(con, "
    SELECT DISTINCT m FROM (
      SELECT channel_month AS m FROM delta WHERE event_type = 'upserted'
      UNION SELECT prev_month FROM delta WHERE prev_month IS NOT NULL)
    WHERE m IS NOT NULL ORDER BY m")$m |> as.Date()
  message("[apply] Betroffene Monats-Chunks: ", paste(format(months, "%Y-%m"), collapse = ", "))

  for (m in as.list(months)) {
    f <- chunk_path(chunk_dir, m)
    m_s <- format(m, "%Y-%m-%d")

    n_ins <- DBI::dbGetQuery(con, sprintf(
      "SELECT COUNT(*) n FROM delta WHERE event_type='upserted' AND channel_month = DATE '%s'", m_s
    ))$n

    # Insert-Zeilen eines Monats: 92 Datenspalten + row_hash (Teil der Identität)
    ins_select <- sprintf("
      SELECT * EXCLUDE (event_type, k_conversion_id, k_touchpoint_id, k_row_hash, prev_month, channel_month),
             k_row_hash AS row_hash
      FROM delta WHERE event_type='upserted' AND channel_month = DATE '%s'", m_s)

    if (!file.exists(f)) {
      if (n_ins == 0) {
        message("[apply] ", format(m, "%Y-%m"), ": kein Chunk, keine Inserts — skip.")
        next
      }
      # Neuer Monat: Chunk direkt aus dem Delta erzeugen
      DBI::dbExecute(con, sprintf("COPY (%s) TO '%s' (FORMAT PARQUET)", ins_select, f))
      message(sprintf("[apply] %s: NEUER Chunk mit %s Zeilen.", format(m, "%Y-%m"), format(n_ins, big.mark = ".")))
      next
    }

    # Entfernen: alle Zeilen-Versionen (conv, tp, hash), deren alte Position
    # dieser Monat ist (deleted + geänderte), plus die upserted-Versionen
    # dieses Monats (macht Wiederholungsläufe nach Crash idempotent).
    kill_select <- sprintf("
      SELECT k_conversion_id, k_touchpoint_id, k_row_hash FROM delta
      WHERE prev_month = DATE '%s'
         OR (event_type='upserted' AND channel_month = DATE '%s')", m_s, m_s)

    tmp <- paste0(f, ".tmp")
    stats <- DBI::dbGetQuery(con, sprintf("
      WITH old AS (SELECT * FROM read_parquet('%s')), kill AS (%s)
      SELECT (SELECT COUNT(*) FROM old) AS n_old,
             (SELECT COUNT(*) FROM old JOIN kill
                ON  old.conversion_id = kill.k_conversion_id
                AND old.touchpoint_id = kill.k_touchpoint_id
                AND old.row_hash      = kill.k_row_hash) AS n_kill", f, kill_select))

    DBI::dbExecute(con, sprintf("
      COPY (
        SELECT old.* FROM read_parquet('%s') old
        ANTI JOIN (%s) kill
          ON  old.conversion_id = kill.k_conversion_id
          AND old.touchpoint_id = kill.k_touchpoint_id
          AND old.row_hash      = kill.k_row_hash
        UNION ALL BY NAME
        %s
      ) TO '%s' (FORMAT PARQUET)", f, kill_select, ins_select, tmp))

    ok <- file.rename(tmp, f) # atomar auf demselben Volume
    if (!ok) stop("[apply] Swap fehlgeschlagen für ", f)
    message(sprintf(
      "[apply] %s: %s Zeilen -> raus %s, rein %s.",
      format(m, "%Y-%m"), format(stats$n_old, big.mark = "."),
      format(stats$n_kill, big.mark = "."), format(n_ins, big.mark = ".")
    ))
  }
  invisible(months)
}

# =============================================================================
# MODUS: delta — nächtlicher inkrementeller Sync
# =============================================================================
run_delta <- function() {
  if (!state_exists()) {
    stop("Sync-State existiert nicht — bitte zuerst 'bootstrap' ausführen.")
  }

  message("=== DELTA-SYNC: berechne Änderungen (1 Full-Scan) ===")

  # 1) Delta serverseitig: FULL OUTER JOIN aktueller Stand vs. Snapshot auf der
  #    vollen Zeilen-Identität (conversion_id, touchpoint_id, row_hash).
  #    upserted = Zeilen-Version neu (mit allen Datenspalten),
  #    deleted  = Zeilen-Version nur noch im Snapshot (Keys + prev_month).
  #    Eine geänderte Zeile erscheint automatisch als deleted + upserted.
  #    DISTINCT schützt das MERGE vor exakten Quell-Doppelzeilen.
  run_query(
    sprintf("
    CREATE OR REPLACE TABLE %s AS
    %s
    SELECT DISTINCT
      CASE WHEN c.row_hash IS NULL THEN 'deleted' ELSE 'upserted' END AS event_type,
      COALESCE(c.conversion_id, s.conversion_id) AS k_conversion_id,
      COALESCE(c.touchpoint_id, s.touchpoint_id) AS k_touchpoint_id,
      COALESCE(c.row_hash,      s.row_hash)      AS k_row_hash,
      s.channel_month AS prev_month,
      c.* EXCEPT (row_hash)
    FROM cur c
    FULL OUTER JOIN %s s
      ON  c.conversion_id = s.conversion_id
      AND c.touchpoint_id = s.touchpoint_id
      AND c.row_hash      = s.row_hash
    WHERE c.row_hash IS NULL
       OR s.row_hash IS NULL", T_DELTA, cur_cte, T_STATE),
    "Delta-Tabelle bauen"
  )

  n_delta <- as.numeric(bigrquery::bq_table_meta(
    bigrquery::bq_table("data-analytics-491117", "adtribute_sync", "int_attribution_delta")
  )$numRows)
  message(sprintf("[delta] %s geänderte Zeilen.", format(n_delta, big.mark = ".")))

  if (n_delta == 0) {
    message("=== Keine Änderungen — fertig. ===")
    return(invisible(NULL))
  }
  if (n_delta > MAX_DELTA_ROWS) {
    stop(sprintf(
      "[delta] %s Zeilen > Limit (%s) — vermutlich Re-Processing bei Adtribute. Bitte 'bootstrap' laufen lassen.",
      format(n_delta, big.mark = "."), format(MAX_DELTA_ROWS, big.mark = ".")
    ))
  }

  # 2) Delta herunterladen + als Staging-Parquet sichern
  tb <- bigrquery::bq_table("data-analytics-491117", "adtribute_sync", "int_attribution_delta")
  df <- download_with_backoff(tb, "Delta")
  assert_hashes_ok(df, "k_row_hash", "Delta")
  staging <- file.path(STAGING_DIR, sprintf("delta_%s.parquet", format(Sys.time(), "%Y%m%d_%H%M%S")))
  arrow::write_parquet(df, staging)
  n_up <- sum(df$event_type == "upserted")
  n_del <- sum(df$event_type == "deleted")
  message(sprintf(
    "[delta] Download OK: %s upserted, %s deleted -> %s",
    format(n_up, big.mark = "."), format(n_del, big.mark = "."), basename(staging)
  ))

  rm(df)
  invisible(gc())

  # 3+4) Lokal anwenden (eigene Funktion, s.u. — auch isoliert testbar)
  months <- apply_delta_locally(staging, CHUNK_DIR)

  # 5) ERST JETZT den Snapshot nachziehen (MERGE, kein Full-Scan der Quelle).
  #    Identität = (conversion_id, touchpoint_id, row_hash): deleted -> raus,
  #    upserted -> rein. Kein UPDATE-Zweig nötig.
  run_query(sprintf(
    "
    MERGE %s t
    USING %s d
    ON  t.conversion_id = d.k_conversion_id
    AND t.touchpoint_id = d.k_touchpoint_id
    AND t.row_hash      = d.k_row_hash
    WHEN MATCHED AND d.event_type = 'deleted' THEN DELETE
    WHEN NOT MATCHED BY TARGET AND d.event_type = 'upserted' THEN
      INSERT (conversion_id, touchpoint_id, channel_month, row_hash)
      VALUES (d.k_conversion_id, d.k_touchpoint_id, d.channel_month, d.k_row_hash)",
    T_STATE, T_DELTA
  ), "Sync-State per MERGE aktualisieren")

  # 5b) Serving-Tabelle (Online-Live-Drilldowns) mit demselben Delta nachziehen.
  #     Fehlt sie (noch nie aufgebaut), wird der Schritt übersprungen.
  serving_exists <- bigrquery::bq_table_exists(bigrquery::bq_table(
    "data-analytics-491117", "adtribute_sync", "int_attribution_serving"
  ))
  if (serving_exists) {
    ins_cols <- paste(SERVING_COLS, collapse = ", ")
    ins_vals <- paste(vapply(SERVING_COLS, function(cn) {
      switch(cn,
        conversion_id = "d.k_conversion_id",
        touchpoint_id = "d.k_touchpoint_id",
        row_hash      = "d.k_row_hash",
        channel_date  = "CAST(d.channel_date AS DATE)",
        paste0("d.", cn)
      )
    }, ""), collapse = ", ")
    run_query(
      sprintf(
        "
      MERGE %s t
      USING %s d
      ON  t.conversion_id = d.k_conversion_id
      AND t.touchpoint_id = d.k_touchpoint_id
      AND t.row_hash      = d.k_row_hash
      WHEN MATCHED AND d.event_type = 'deleted' THEN DELETE
      WHEN NOT MATCHED BY TARGET AND d.event_type = 'upserted' THEN
        INSERT (%s) VALUES (%s)",
        T_SERVING, T_DELTA, ins_cols, ins_vals
      ),
      "Serving-Tabelle per MERGE aktualisieren"
    )
  } else {
    message("[delta] Serving-Tabelle existiert nicht — übersprungen (Modus 'serving' zum Aufbau).")
  }

  # 6) Staging aufräumen (letzte 7 behalten für Debugging)
  old_files <- sort(list.files(STAGING_DIR, pattern = "^delta_.*parquet$", full.names = TRUE),
    decreasing = TRUE
  )
  if (length(old_files) > 7) unlink(old_files[-(1:7)])

  message(sprintf(
    "=== DELTA-SYNC FERTIG: %s upserted, %s deleted, %d Chunk(s) aktualisiert. ===",
    format(n_up, big.mark = "."), format(n_del, big.mark = "."), length(months)
  ))
}

# =============================================================================
# MODUS: serving — (Neu-)Aufbau der Online-Serving-Tabelle aus dem View
# (1 Full-Scan ~1,5 $; danach hält der nächtliche Delta-MERGE sie aktuell)
# =============================================================================
run_serving <- function() {
  run_query(
    sprintf("
    CREATE OR REPLACE TABLE %s
    PARTITION BY channel_date
    CLUSTER BY shopify_order_id
    AS %s SELECT %s FROM cur", T_SERVING, cur_cte, serving_select),
    "Serving-Tabelle aufbauen (partitioniert + geclustert)"
  )
  meta <- bigrquery::bq_table_meta(bigrquery::bq_table(
    "data-analytics-491117", "adtribute_sync", "int_attribution_serving"
  ))
  message(sprintf(
    "[serving] %s Zeilen, %.1f GB logisch.",
    format(as.numeric(meta$numRows), big.mark = "'"),
    as.numeric(meta$numBytes) / 1e9
  ))
}

# =============================================================================
# MODUS: repair — prüft je Monat Anzahl + BIT_XOR(row_hash) lokal vs. Sync-State
# und entfernt lokale Waisen-Zeilen (Identität nicht im State). Fehlen lokal
# Zeilen (n_local < n_state), hilft nur ein bootstrap — wird gemeldet.
# =============================================================================
run_repair <- function() {
  if (!state_exists()) stop("Sync-State existiert nicht — bitte zuerst 'bootstrap'.")

  # Hashes als STRING vergleichen (integer64 vs. double wäre lossy -> false positives)
  st <- bigrquery::bq_table_download(bigrquery::bq_project_query(BILLING, sprintf(
    "SELECT channel_month, COUNT(*) n, CAST(BIT_XOR(row_hash) AS STRING) hx FROM %s GROUP BY 1", T_STATE
  ),
  quiet = TRUE
  ), bigint = "integer64", quiet = TRUE)

  con <- duck_con()
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
  loc <- DBI::dbGetQuery(con, sprintf("
    SELECT strptime(parse_filename(filename)[15:21] || '_01', '%%Y_%%m_%%d')::DATE AS channel_month,
           COUNT(*) n, BIT_XOR(row_hash)::VARCHAR hx
    FROM read_parquet('%s/*.parquet', filename=true) GROUP BY 1", CHUNK_DIR))

  cmp <- merge(st, loc, by = "channel_month", suffixes = c("_state", "_local"))
  bad <- cmp[as.numeric(cmp$n_state) != as.numeric(cmp$n_local) |
    cmp$hx_state != cmp$hx_local, ]
  if (nrow(bad) == 0) {
    message("[repair] ✅ Alles konsistent — nichts zu tun.")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(bad))) {
    m <- as.Date(bad$channel_month[i])
    f <- chunk_path(CHUNK_DIR, m)
    message(sprintf(
      "[repair] %s: state=%s local=%s", format(m, "%Y-%m"),
      format(as.numeric(bad$n_state[i]), big.mark = "'"),
      format(as.numeric(bad$n_local[i]), big.mark = "'")
    ))
    # State-Identitäten des Monats holen und lokal SEMI-joinen (Waisen fliegen raus)
    keys <- download_with_backoff(bigrquery::bq_project_query(BILLING, sprintf(
      "SELECT conversion_id, touchpoint_id, row_hash FROM %s WHERE channel_month = DATE '%s'",
      T_STATE, format(m, "%Y-%m-%d")
    ), quiet = TRUE), paste("State-Keys", format(m, "%Y-%m")))
    kf <- file.path(STAGING_DIR, "repair_keys.parquet")
    arrow::write_parquet(keys, kf)
    rm(keys)
    invisible(gc())

    tmp <- paste0(f, ".tmp")
    DBI::dbExecute(con, sprintf("
      COPY (
        SELECT old.* FROM read_parquet('%s') old
        SEMI JOIN read_parquet('%s') k
          ON  old.conversion_id = k.conversion_id
          AND old.touchpoint_id = k.touchpoint_id
          AND old.row_hash      = k.row_hash
      ) TO '%s' (FORMAT PARQUET)", f, kf, tmp))
    n_new <- DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) n FROM read_parquet('%s')", tmp))$n
    if (n_new < as.numeric(bad$n_state[i])) {
      message(
        "[repair] ⚠️ ", format(m, "%Y-%m"), ": lokal FEHLEN Zeilen (", n_new, " < ",
        bad$n_state[i], ") — bootstrap nötig!"
      )
    }
    file.rename(tmp, f)
    unlink(kf)
    message(sprintf(
      "[repair] %s: %s -> %s Zeilen.", format(m, "%Y-%m"),
      format(as.numeric(bad$n_local[i]), big.mark = "'"), format(n_new, big.mark = "'")
    ))
  }
  message("[repair] fertig — Validierung erneut laufen lassen.")
}

# --- Main ----------------------------------------------------------------------
# (SYNC_NO_MAIN vor dem source() setzen, um nur die Funktionen zu laden — Tests)
if (!exists("SYNC_NO_MAIN")) {
  bigrquery::bq_auth(path = JSON_KEY)
  message("BigQuery-Auth OK (", MODE, "-Modus).")
  t0 <- Sys.time()
  if (MODE == "bootstrap") {
    run_bootstrap()
  } else if (MODE == "repair") {
    run_repair()
  } else if (MODE == "serving") {
    run_serving()
  } else {
    run_delta()
  }
  message(sprintf("Gesamtlaufzeit: %.1f min", as.numeric(difftime(Sys.time(), t0, units = "mins"))))
}
