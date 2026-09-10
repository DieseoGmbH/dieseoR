# ============================================================================
# backfill_shopify_finance_columns.R
# ----------------------------------------------------------------------------
# EINMALIGER Rebuild der Master-`orders`-Tabelle, damit ALLE (auch neue)
# Spalten aus clean_up_shopify() fuer die GESAMTE Historie befuellt sind:
#   gross_sales, discount_amount, returned_amount, returned_quantity,
#   returned_tax, net_sales, net_tax, shipping_charges, return_fees,
#   total_sales, shipping_address_country_code, first_name, last_name, ...
#
# Zwei Modi (MODE):
#   "local" (Default): reprocesst die bereits vorhandenen raw_chunks von der
#            Platte  ->  KEIN erneuter API-Fetch (schnell). Voraussetzung: ein
#            vollstaendiger Full-Fetch liegt bereits als raw_chunks vor.
#   "api":   fetcht fensterweise (created_at pro Monat) frisch von Shopify.
#
# Robustheit:
#   * orders_rebuild erbt die Typen der Master-`orders` (41 Spalten).
#   * ALLE weiteren Spalten (Finance, first_name/last_name, kuenftige) werden
#     per SCHEMA-EVOLUTION aus den R-Typen ergaenzt -> keine feste Spaltenliste,
#     kein Bruch, wenn clean_up_shopify() erweitert wird.
#   * Insert je Chunk via Staging + INSERT ... BY NAME (castet auf Zieltypen;
#     immun gegen per-Chunk-Typinferenz, z.B. all-NA `note`).
#   * RESUME-faehig (bereits verarbeitete Chunks/Fenster werden uebersprungen).
#   * Finaler Dedupe (order_id,item_id -> juengstes updated_at) => idempotent.
#   * Atomarer Datei-Swap; Alt-Master nach ~/.Trash (kein rm).
#
# VORAUSSETZUNGEN:
#   * load_all()/installierte dieseoR-Version mit aktuellem clean_up_shopify().
#   * ~20 GB freier Speicher (Schatten-DB + Rebuild-Tabelle transient).
#   * MODE="api": .Renviron mit SHOPIFY_CLIENT_ID, SHOPIFY_CLIENT_SECRET.
#
# Bei Abbruch einfach erneut starten -> setzt fort.
# ============================================================================

suppressPackageStartupMessages({
  library(devtools)
  load_all("~/git/dieseoR")
  library(DBI)
  library(duckdb)
  library(dplyr)
  library(lubridate)
})

# ---- Konfiguration ---------------------------------------------------------
MODE <- "local" # "local" | "api"
datadir <- path.expand("~/data")
db_path <- file.path(datadir, "shopify", "shopify.duckdb")
shadow_path <- file.path(datadir, "shopify", "shopify_backfill.duckdb")
raw_dir <- file.path(datadir, "shopify", "raw_chunks")
window_len <- months(1) # nur MODE="api": Fenstergroesse

stopifnot(file.exists(db_path))

# R-Klasse -> DuckDB-Typ (wie perform_duckdb_upsert)
sql_type <- function(rclass) {
  switch(rclass,
    integer = "INTEGER",
    numeric = "DOUBLE",
    logical = "BOOLEAN",
    POSIXct = "TIMESTAMP",
    POSIXt = "TIMESTAMP",
    Date = "DATE",
    character = "VARCHAR",
    "VARCHAR"
  )
}

# ---- 1. Schatten-DB (Kopie mit ALLEN Tabellen); nur anlegen wenn neu -------
if (!file.exists(shadow_path)) {
  message("Erstelle Schatten-DB (Kopie des Masters, inkl. aller Tabellen) ...")
  file.copy(db_path, shadow_path, overwrite = FALSE)
}
con <- DBI::dbConnect(duckdb::duckdb(), dbdir = shadow_path, read_only = FALSE)

if (!DBI::dbExistsTable(con, "backfill_progress")) {
  DBI::dbExecute(con, "CREATE TABLE backfill_progress (item VARCHAR)")
}
# Ziel-Tabelle mit KORREKTEM Master-Schema (41 Spalten, korrekte Typen).
# Weitere Spalten kommen per Schema-Evolution im Verlauf dazu.
if (!DBI::dbExistsTable(con, "orders_rebuild")) {
  DBI::dbExecute(con, "CREATE TABLE orders_rebuild AS SELECT * FROM orders LIMIT 0")
}
done <- DBI::dbGetQuery(con, "SELECT item FROM backfill_progress")$item

# ---- Kernfunktion: ein Roh-Dataframe -> cleanen -> in orders_rebuild -------
process_df <- function(df_raw) {
  df_clean <- dieseoR::clean_up_shopify(df_raw, endpoint = "orders")
  if (nrow(df_clean) == 0) {
    return(0L)
  }

  # Schema-Evolution: Spalten, die orders_rebuild noch nicht hat, typkorrekt anlegen.
  tgt <- DBI::dbListFields(con, "orders_rebuild")
  miss <- setdiff(names(df_clean), tgt)
  for (cn in miss) {
    sqlt <- sql_type(class(df_clean[[cn]])[1])
    # all-NA-logical ist ein Inferenz-Artefakt -> sicher als VARCHAR anlegen
    if (is.logical(df_clean[[cn]]) && all(is.na(df_clean[[cn]]))) sqlt <- "VARCHAR"
    DBI::dbExecute(con, sprintf('ALTER TABLE orders_rebuild ADD COLUMN "%s" %s', cn, sqlt))
    message(sprintf("    + Schema-Evolution: Spalte '%s' (%s)", cn, sqlt))
  }

  DBI::dbWriteTable(con, "win_staging", as.data.frame(df_clean), overwrite = TRUE)
  DBI::dbExecute(con, "INSERT INTO orders_rebuild BY NAME SELECT * FROM win_staging")
  DBI::dbRemoveTable(con, "win_staging")
  nrow(df_clean)
}

# ---- 2. Datenquelle abarbeiten ---------------------------------------------
if (MODE == "local") {
  chunk_files <- sort(list.files(raw_dir, pattern = "^shopify_orders_.*\\.rds$", full.names = TRUE))
  total <- length(chunk_files)
  message(sprintf("MODE=local: reprocesse %d orders-Chunks von der Platte.", total))

  i <- 0L
  for (f in chunk_files) {
    i <- i + 1L
    key <- basename(f)
    if (key %in% done) {
      next
    }
    df_raw <- tryCatch(readRDS(f), error = function(e) {
      message("  ❌ Lesefehler ", key, ": ", e$message)
      NULL
    })
    if (is.null(df_raw)) next
    n <- process_df(df_raw)
    rm(df_raw)
    gc()
    DBI::dbExecute(con, sprintf("INSERT INTO backfill_progress VALUES ('%s')", key))
    message(sprintf("  [%d/%d] %s -> %d Zeilen", i, total, key, n))
  }
  done_now <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM backfill_progress")$n
  all_done <- done_now >= total
} else if (MODE == "api") {
  con_m <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  rng <- DBI::dbGetQuery(con_m, "SELECT min(created_at) mn, max(created_at) mx FROM orders")
  DBI::dbDisconnect(con_m, shutdown = TRUE)
  start_date <- if (is.na(rng$mn)) as.Date("2024-01-01") else as.Date(rng$mn)
  end_date <- if (is.na(rng$mx)) Sys.Date() else as.Date(rng$mx)
  start_date <- lubridate::floor_date(start_date, "month")
  window_starts <- seq(start_date, lubridate::floor_date(end_date, "month"), by = "1 month")
  total <- length(window_starts)
  message(sprintf("MODE=api: %s .. %s in %d Monatsfenstern.", start_date, end_date, total))

  iso <- function(d) format(as.POSIXct(paste0(as.Date(d), " 00:00:00"), tz = "UTC"), "%Y-%m-%dT%H:%M:%S%z")
  for (ws in as.list(window_starts)) {
    ws <- as.Date(ws)
    key <- as.character(ws)
    if (key %in% done) {
      message("  ueberspringe (erledigt): ", key)
      next
    }
    we <- ws %m+% window_len
    message(sprintf("\n=== Fenster %s .. %s ===", ws, we))
    token <- dieseoR::get_shopify_token(
      client_id = Sys.getenv("SHOPIFY_CLIENT_ID"), client_secret = Sys.getenv("SHOPIFY_CLIENT_SECRET")
    )
    df_raw <- tryCatch(
      dieseoR::get_shopify_data(
        api_key = token, endpoint = "orders", raw_dir = raw_dir,
        created_at_min = iso(ws), created_at_max = iso(we)
      ),
      error = function(e) {
        message("  ❌ Fetch-Fehler: ", e$message)
        NULL
      }
    )
    if (is.null(df_raw)) next # Fehler -> nicht als erledigt markieren
    n <- if (nrow(df_raw) == 0) 0L else process_df(df_raw)
    rm(df_raw)
    gc()
    DBI::dbExecute(con, sprintf("INSERT INTO backfill_progress VALUES ('%s')", key))
    message(sprintf("  -> %d Item-Zeilen", n))
  }
  all_done <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM backfill_progress")$n >= total
} else {
  DBI::dbDisconnect(con, shutdown = TRUE)
  stop("Unbekannter MODE: ", MODE)
}

# ---- 3. Finalisieren: dedupe + orders ersetzen + Datei-Swap ----------------
if (isTRUE(all_done) && DBI::dbExistsTable(con, "orders_rebuild")) {
  message("\nAlle Quellen verarbeitet. Dedupliziere (order_id,item_id) und ersetze `orders` ...")

  old_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM orders")$n
  new_n <- DBI::dbGetQuery(con, "SELECT COUNT(*) n FROM (SELECT 1 FROM orders_rebuild GROUP BY order_id, item_id)")$n
  message(sprintf("  orders alt: %s  ->  neu (dedupliziert): %s", old_n, new_n))

  DBI::dbExecute(con, "DROP TABLE orders")
  DBI::dbExecute(con, "
    CREATE TABLE orders AS
    SELECT * EXCLUDE (rn) FROM (
      SELECT *, row_number() OVER (PARTITION BY order_id, item_id ORDER BY updated_at DESC) AS rn
      FROM orders_rebuild
    ) WHERE rn = 1
  ")
  DBI::dbExecute(con, "DROP TABLE orders_rebuild")
  DBI::dbExecute(con, "DROP TABLE backfill_progress")
  DBI::dbDisconnect(con, shutdown = TRUE)

  ts <- format(Sys.time(), "%Y%m%d_%H%M%S")
  trash <- path.expand(sprintf("~/.Trash/shopify_duckdb_prebackfill_%s.duckdb", ts))
  file.rename(db_path, trash)
  file.rename(shadow_path, db_path)
  message("✅ Backfill abgeschlossen. Alt-Master gesichert unter: ", trash)
  message("   Naechster Nachtlauf schneidet den Data Mart inkl. der neuen Spalten frisch.")
} else {
  DBI::dbDisconnect(con, shutdown = TRUE)
  message("⏸ Noch nicht alle Quellen fertig. Skript erneut starten zum Fortsetzen.")
}
