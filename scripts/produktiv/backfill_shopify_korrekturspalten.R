# ==========================================================================
# BACKFILL der Korrektur-Spalten in der Shopify-Master-DuckDB
# --------------------------------------------------------------------------
# Fuellt die Spalten, die `clean_up_shopify()` seit Juli 2026 zusaetzlich
# erzeugt, fuer die historischen Orders nach. Quelle sind die Roh-Chunks im
# Data Lake (`~/data/shopify/raw_chunks/`) — es wird KEIN API-Abruf gemacht.
#
# Nachgefuellte Spalten:
#   order_tax_lines_total      Order-Steuer aus tax_lines (statt total_tax)
#   total_tax_effective        total_tax, bei 0 ersetzt durch tax_lines-Summe
#   tax_rates_n                Anzahl verschiedener Steuersaetze der Order
#   tax_rate_primary           dominanter Steuersatz
#   discount_amount_allocated  vollstaendige Positions-Rabatte (discount_allocations)
#   line_item_tax              Steuer je Position
#   refund_adjustments_total   Summe der order_adjustments (Roh)
#   refund_shipping_total      davon kind = 'shipping_refund'
#
# ⚠️ REICHWEITE: Der Data Lake beginnt beim Volllauf vom 09.06.2026 und reicht
#    damit bis etwa 2024-07-14 zurueck. Aeltere Orders (2022, 2023, H1 2024)
#    sind NICHT im Lake und bleiben NULL. Fuer sie braeuchte es einen erneuten
#    API-Lauf, z. B.
#      get_shopify_data(endpoint = "orders", api_key = tok,
#                       created_at_max = "2024-07-14T00:00:00Z")
#
# ⚠️ SCHREIBT AUF DIE MASTER-DB. Arbeitsweise wie `update_shopify_data()`:
#    Blue-Green auf einer Schattenkopie, am Ende atomarer Datei-Tausch. Die
#    Live-DB bleibt bis zum Schluss unangetastet und lesbar.
#
# Laufzeit: ~560 Chunks, grob 1-3 Stunden. Resumable: bereits verarbeitete
# Chunks stehen im State-File und werden uebersprungen.
#
# Aufruf:
#   Rscript ~/git/dieseoR/scripts/produktiv/backfill_shopify_korrekturspalten.R
#   Rscript ... --dry-run     # nur zaehlen, nichts schreiben
# ==========================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(dplyr)
})

devtools::load_all("~/git/dieseoR", quiet = TRUE)

args <- commandArgs(trailingOnly = TRUE)
DRY_RUN <- "--dry-run" %in% args

DATADIR <- path.expand("~/data/shopify")
DB_LIVE <- file.path(DATADIR, "shopify.duckdb")
DB_SHADOW <- file.path(DATADIR, "shopify_backfill.duckdb")
RAW_DIR <- file.path(DATADIR, "raw_chunks")
STATE_FILE <- file.path(DATADIR, "backfill_korrekturspalten_state.rds")

NEUE_SPALTEN <- c(
  order_tax_lines_total     = "DOUBLE",
  total_tax_effective       = "DOUBLE",
  tax_rates_n               = "INTEGER",
  tax_rate_primary          = "DOUBLE",
  discount_amount_allocated = "DOUBLE",
  line_item_tax             = "DOUBLE",
  refund_adjustments_total  = "DOUBLE",
  refund_shipping_total     = "DOUBLE"
)

log_msg <- function(...) message(format(Sys.time(), "[%H:%M:%S] "), ...)

# --------------------------------------------------------------------------
# 1. Chunks bestimmen (aelteste zuerst, damit spaetere Updates gewinnen)
# --------------------------------------------------------------------------
chunks <- list.files(RAW_DIR, pattern = "^shopify_orders_.*\\.rds$", full.names = TRUE)
if (length(chunks) == 0) stop("Keine Roh-Chunks in ", RAW_DIR, call. = FALSE)
chunks <- chunks[order(file.info(chunks)$mtime)]

state <- if (file.exists(STATE_FILE)) readRDS(STATE_FILE) else character(0)
offen <- setdiff(basename(chunks), state)
log_msg(sprintf(
  "Chunks gesamt: %d | bereits erledigt: %d | offen: %d",
  length(chunks), length(state), length(offen)
))

if (DRY_RUN) {
  log_msg("DRY RUN — es wird nichts geschrieben.")
  gr <- range(file.info(chunks)$mtime)
  log_msg("Chunk-Zeitraum (mtime): ", gr[1], " bis ", gr[2])
  quit(save = "no", status = 0)
}
if (length(offen) == 0) {
  log_msg("Nichts zu tun. State-File loeschen, um von vorn zu beginnen.")
  quit(save = "no", status = 0)
}

# --------------------------------------------------------------------------
# 2. Schattenkopie anlegen (Blue-Green)
# --------------------------------------------------------------------------
if (!file.exists(DB_SHADOW)) {
  log_msg("Kopiere Master-DB in die Schatten-DB (das dauert bei ~8 GB einige Minuten)...")
  ok <- file.copy(DB_LIVE, DB_SHADOW, overwrite = TRUE)
  if (!ok) stop("Kopie der DuckDB fehlgeschlagen.", call. = FALSE)
  log_msg("Kopie fertig.")
} else {
  log_msg("Schatten-DB existiert bereits — setze fort.")
}

con <- dbConnect(duckdb::duckdb(), dbdir = DB_SHADOW, read_only = FALSE)
on.exit(try(dbDisconnect(con, shutdown = TRUE), silent = TRUE), add = TRUE)

# --------------------------------------------------------------------------
# 3. Spalten anlegen, falls noch nicht vorhanden
# --------------------------------------------------------------------------
vorhanden <- dbListFields(con, "orders")
for (sp in names(NEUE_SPALTEN)) {
  if (!sp %in% vorhanden) {
    dbExecute(con, sprintf('ALTER TABLE orders ADD COLUMN "%s" %s', sp, NEUE_SPALTEN[[sp]]))
    log_msg("Spalte angelegt: ", sp)
  }
}

# --------------------------------------------------------------------------
# 4. Chunk fuer Chunk: bereinigen, stagen, per Primary Key updaten
# --------------------------------------------------------------------------
set_sql <- paste(sprintf('"%s" = s."%s"', names(NEUE_SPALTEN), names(NEUE_SPALTEN)),
  collapse = ",\n    "
)

verarbeitet <- 0L
zeilen_total <- 0L

for (fn in offen) {
  pfad <- file.path(RAW_DIR, fn)
  res <- tryCatch(
    {
      roh <- readRDS(pfad)
      if (nrow(roh) == 0) {
        log_msg(fn, ": leer, uebersprungen")
        list(n = 0L)
      } else {
        cl <- clean_up_shopify(roh, endpoint = "orders")
        rm(roh)
        gc(verbose = FALSE)

        # Nur PK + Korrekturspalten uebertragen; item_id kann bei Orders ohne
        # Line Items NA sein -> solche Zeilen sind per PK nicht adressierbar.
        upd <- cl |>
          dplyr::select(dplyr::all_of(c("order_id", "item_id", names(NEUE_SPALTEN)))) |>
          dplyr::filter(!is.na(order_id), !is.na(item_id))
        rm(cl)
        gc(verbose = FALSE)

        dbWriteTable(con, "backfill_staging", upd, overwrite = TRUE)
        n <- dbExecute(con, sprintf("
        UPDATE orders AS o
        SET %s
        FROM backfill_staging AS s
        WHERE o.order_id = s.order_id AND o.item_id = s.item_id", set_sql))
        dbRemoveTable(con, "backfill_staging")
        rm(upd)
        gc(verbose = FALSE)
        list(n = n)
      }
    },
    error = function(e) {
      log_msg("❌ FEHLER bei ", fn, ": ", conditionMessage(e))
      NULL
    }
  )

  if (is.null(res)) next # Fehler: Chunk NICHT als erledigt markieren

  state <- c(state, fn)
  saveRDS(state, STATE_FILE)
  verarbeitet <- verarbeitet + 1L
  zeilen_total <- zeilen_total + res$n
  if (verarbeitet %% 10 == 0 || verarbeitet == length(offen)) {
    log_msg(sprintf(
      "%d/%d Chunks | %s Zeilen aktualisiert",
      verarbeitet, length(offen), format(zeilen_total, big.mark = ".")
    ))
  }
}

# --------------------------------------------------------------------------
# 5. Kontrolle vor dem Tausch
# --------------------------------------------------------------------------
kontrolle <- dbGetQuery(con, "
  WITH ord AS (
    SELECT order_id,
           any_value(created_at)             AS created_at,
           any_value(total_tax)              AS total_tax,
           any_value(total_tax_effective)    AS total_tax_effective,
           any_value(total_discounts)        AS total_discounts,
           SUM(discount_amount)              AS discount_amount,
           SUM(discount_amount_allocated)    AS discount_amount_allocated
    FROM orders WHERE created_at IS NOT NULL GROUP BY order_id
  )
  SELECT YEAR(created_at) AS jahr, COUNT(*) AS orders,
         SUM(CASE WHEN total_tax_effective IS NULL THEN 1 ELSE 0 END) AS ohne_backfill,
         ROUND(SUM(total_tax))                       AS tax_alt,
         ROUND(SUM(total_tax_effective))             AS tax_neu,
         ROUND(SUM(total_discounts))                 AS rabatt_wahrheit,
         ROUND(SUM(discount_amount))                 AS rabatt_alt,
         ROUND(SUM(discount_amount_allocated))       AS rabatt_neu
  FROM ord GROUP BY 1 ORDER BY 1")
log_msg("Kontrolle nach Backfill:")
# capture.output() statt print(): der pre-commit-Hook "no-print-statement"
# verbietet print() -- Ausgaben sollen ueber cat()/message() laufen.
cat(paste(capture.output(kontrolle), collapse = "\n"), "\n")

dbDisconnect(con, shutdown = TRUE)

# --------------------------------------------------------------------------
# 6. Atomarer Tausch
# --------------------------------------------------------------------------
if (length(setdiff(basename(chunks), state)) > 0) {
  log_msg("⚠️ Es sind noch Chunks offen — KEIN Tausch. Skript erneut starten.")
  quit(save = "no", status = 1)
}

backup <- paste0(DB_LIVE, ".vor_backfill_", format(Sys.time(), "%Y%m%d_%H%M%S"))
log_msg("Sichere Live-DB nach ", basename(backup))
file.rename(DB_LIVE, backup)
file.rename(DB_SHADOW, DB_LIVE)
log_msg(
  "✅ Tausch erledigt. Alte DB liegt als ", basename(backup),
  " daneben — nach Sichtprüfung nach ~/.Trash/ verschieben."
)
log_msg("Danach: build_schema_snapshot.R laufen lassen, damit die Doku stimmt.")
