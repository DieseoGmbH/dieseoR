# ==========================================================================
# Roh-Chunks bereinigen und in die Master-DuckDB upserten — OHNE API-Abruf
# --------------------------------------------------------------------------
# Setzt genau dort ein, wo `update_shopify_data()` nach dem Fetch weitermacht
# ("🔄 ... neue/geaenderte Datensaetze gefunden. Starte Bereinigung..."). Nutzt
# man, wenn der Fetch schon durchgelaufen ist und nur die Bereinigung noch
# fehlt — dann muss man die Seiten nicht erneut ziehen.
#
# Der entscheidende Unterschied zu `update_shopify_data()`: hier wird
# **Chunk fuer Chunk** bereinigt und upsertet, nicht der gesamte Delta in einem
# Zug. `clean_up_shopify()` braucht pro 25.000 Orders rund 400 MB
# Zwischenspeicher; bei 110.000 Orders am Stueck sind das mehrere GB und die
# Maschine swappt. Blockweise bleibt der Bedarf konstant.
#
# Idempotent: Der Upsert loescht die betroffenen (order_id, item_id) und fuegt
# sie neu ein. Ein zweiter Lauf aendert nichts. Bereits erledigte Chunks stehen
# im State-File und werden uebersprungen.
#
# Aufruf (aus dem dieseoR-Projekt, kein install nötig — load_all reicht):
#   Rscript ~/git/dieseoR/scripts/produktiv/clean_and_upsert_shopify_chunks.R
#   Rscript ... --run-id=20260803_102705      # bestimmten Lauf waehlen
#   Rscript ... --dry-run                     # nur anzeigen, was gemacht wuerde
#   Rscript ... --reset                       # State-File verwerfen, von vorn
# ==========================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(dplyr)
})
devtools::load_all("~/git/dieseoR", quiet = TRUE)

args <- commandArgs(trailingOnly = TRUE)
DRY <- "--dry-run" %in% args
RESET <- "--reset" %in% args
RUN_ARG <- sub("^--run-id=", "", grep("^--run-id=", args, value = TRUE))

DATADIR <- path.expand("~/data/shopify")
DB <- file.path(DATADIR, "shopify.duckdb")
RAW_DIR <- file.path(DATADIR, "raw_chunks")
ENDPOINT <- "orders"
PK <- c("order_id", "item_id")

log_msg <- function(...) message(format(Sys.time(), "[%H:%M:%S] "), ...)

# --------------------------------------------------------------------------
# 1. Lauf bestimmen: entweder --run-id oder der neueste orders-Lauf
# --------------------------------------------------------------------------
alle <- list.files(RAW_DIR, pattern = "^shopify_orders_.*_chunk_\\d+\\.rds$", full.names = TRUE)
if (length(alle) == 0) stop("Keine orders-Chunks in ", RAW_DIR, call. = FALSE)

run_ids <- unique(sub("^shopify_orders_(\\d{8}_\\d{6})_chunk_.*$", "\\1", basename(alle)))
RUN_ID <- if (length(RUN_ARG) == 1 && nzchar(RUN_ARG)) {
  if (!RUN_ARG %in% run_ids) stop("Run-ID nicht gefunden: ", RUN_ARG, call. = FALSE)
  RUN_ARG
} else {
  sort(run_ids, decreasing = TRUE)[1]
}

chunks <- sort(alle[grepl(sprintf("shopify_orders_%s_chunk_", RUN_ID), basename(alle))])
STATE_FILE <- file.path(DATADIR, sprintf("clean_upsert_state_%s.rds", RUN_ID))
if (RESET && file.exists(STATE_FILE)) file.remove(STATE_FILE)

state <- if (file.exists(STATE_FILE)) readRDS(STATE_FILE) else character(0)
offen <- setdiff(basename(chunks), state)

log_msg("Lauf: ", RUN_ID)
log_msg(sprintf(
  "Chunks: %d | erledigt: %d | offen: %d",
  length(chunks), length(state), length(offen)
))
log_msg(sprintf("Rohdaten: %.0f MB", sum(file.info(chunks)$size) / 1024^2))

if (DRY) {
  cat("\nZu verarbeiten:\n")
  cat(paste0("  ", offen, collapse = "\n"), "\n")
  log_msg("DRY RUN — es wird nichts geschrieben.")
  quit(save = "no", status = 0)
}
if (length(offen) == 0) {
  log_msg("Nichts zu tun. Mit --reset von vorn beginnen.")
  quit(save = "no", status = 0)
}

# --------------------------------------------------------------------------
# 2. Direkt auf die Master-DB schreiben.
#    Kein Blue-Green: der Upsert ist idempotent, DuckDB ist ACID, und eine
#    aktuelle Sicherung liegt vom Backfill daneben. Das spart die 8-GB-Kopie.
#    ⚠️ Solange das Skript laeuft, darf nichts anderes die DB oeffnen —
#    DuckDB erlaubt entweder EINEN Schreiber oder mehrere Leser.
# --------------------------------------------------------------------------
sicherungen <- list.files(DATADIR, pattern = "^shopify\\.duckdb\\.(vor_backfill|bak)", full.names = TRUE)
if (length(sicherungen) == 0) {
  log_msg("⚠️ Keine Sicherung der Master-DB gefunden. Abbruch — erst sichern:")
  log_msg("   cp ~/data/shopify/shopify.duckdb ~/data/shopify/shopify.duckdb.bak")
  quit(save = "no", status = 1)
}
log_msg("Sicherung vorhanden: ", basename(sicherungen[1]))

con <- dbConnect(duckdb::duckdb(), dbdir = DB, read_only = FALSE)
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

vor <- dbGetQuery(con, "SELECT COUNT(*) n, MAX(updated_at) mx FROM orders")
log_msg(sprintf(
  "DB vorher: %s Zeilen | max(updated_at) = %s",
  format(vor$n, big.mark = "."), format(vor$mx)
))

# --------------------------------------------------------------------------
# 3. Chunk fuer Chunk: bereinigen -> upserten -> Speicher freigeben
# --------------------------------------------------------------------------
verarbeitet <- 0L
zeilen_total <- 0L

for (fn in offen) {
  t0 <- Sys.time()
  ok <- tryCatch(
    {
      roh <- readRDS(file.path(RAW_DIR, fn))
      n_orders <- nrow(roh)

      df_clean <- clean_up_shopify(shopify_data = roh, endpoint = ENDPOINT)
      rm(roh)
      gc(verbose = FALSE)

      # Zeilen ohne Primary Key sind nicht adressierbar (Orders ohne Line Items)
      n_ohne_pk <- sum(is.na(df_clean$order_id) | is.na(df_clean$item_id))
      if (n_ohne_pk > 0) {
        df_clean <- df_clean |> dplyr::filter(!is.na(order_id), !is.na(item_id))
      }

      perform_duckdb_upsert(con, df_clean, ENDPOINT, PK)
      n_rows <- nrow(df_clean)
      rm(df_clean)
      gc(verbose = FALSE)

      log_msg(sprintf(
        "  %-46s %6d Orders -> %6d Zeilen  (%.0f s%s)",
        fn, n_orders, n_rows,
        as.numeric(difftime(Sys.time(), t0, units = "secs")),
        if (n_ohne_pk > 0) sprintf(", %d ohne PK verworfen", n_ohne_pk) else ""
      ))
      zeilen_total <- zeilen_total + n_rows
      TRUE
    },
    error = function(e) {
      log_msg("❌ FEHLER bei ", fn, ": ", conditionMessage(e))
      FALSE
    }
  )

  if (!ok) next # Chunk NICHT als erledigt markieren -> naechster Lauf holt ihn
  state <- c(state, fn)
  saveRDS(state, STATE_FILE)
  verarbeitet <- verarbeitet + 1L
}

# --------------------------------------------------------------------------
# 4. Kontrolle
# --------------------------------------------------------------------------
nach <- dbGetQuery(con, "SELECT COUNT(*) n, MAX(updated_at) mx FROM orders")
log_msg(sprintf(
  "DB nachher: %s Zeilen (%+d) | max(updated_at) = %s",
  format(nach$n, big.mark = "."), nach$n - vor$n, format(nach$mx)
))

kontrolle <- dbGetQuery(con, "
  WITH ord AS (
    SELECT order_id, any_value(created_at) AS ca,
           any_value(total_discounts) AS td, any_value(total_tax) AS tt,
           any_value(total_tax_effective) AS tte,
           SUM(discount_amount_allocated) AS da
    FROM orders WHERE created_at >= DATE '2026-07-01' GROUP BY order_id)
  SELECT strftime(ca, '%Y-%m') AS monat, COUNT(*) AS orders,
         SUM(CASE WHEN tte IS NULL THEN 1 ELSE 0 END) AS ohne_korrekturspalten,
         SUM(CASE WHEN abs(da - td) < 0.02 THEN 1 ELSE 0 END) AS rabatt_ok,
         ROUND(SUM(tt)) AS steuer_alt, ROUND(SUM(tte)) AS steuer_neu
  FROM ord GROUP BY 1 ORDER BY 1")
cat("\n")
# capture.output() statt print(): der pre-commit-Hook "no-print-statement"
# verbietet print() -- Ausgaben sollen ueber cat()/message() laufen, damit sie
# sich unterdruecken lassen. Die Zeilennummern links sind der Preis dafuer.
cat(paste(capture.output(kontrolle), collapse = "\n"), "\n")

dbDisconnect(con, shutdown = TRUE)

log_msg(sprintf(
  "✅ Fertig: %d Chunks, %s Zeilen upsertet.",
  verarbeitet, format(zeilen_total, big.mark = ".")
))
if (length(setdiff(basename(chunks), state)) == 0) {
  log_msg("Alle Chunks dieses Laufs erledigt. State-File kann weg:")
  log_msg("   mv ", STATE_FILE, " ~/.Trash/")
}
log_msg("Weiter in update_dashboard.R ab Schritt 5 (Trustpilot) — Schritt 4 ist damit erledigt.")
