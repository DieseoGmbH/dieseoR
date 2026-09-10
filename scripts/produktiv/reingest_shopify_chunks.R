# ==============================================================================
# reingest_shopify_chunks.R  --  Shopify-Delta aus vorhandenen Roh-Chunks
#                                nachtraeglich in den Master einspielen.
# ==============================================================================
#
# WOFUER: Wenn `update_shopify_data()` die Daten zwar geholt und bereinigt hat,
# der Blue-Green-Swap am Ende aber nicht durchlief, liegen die Roh-Chunks des
# Runs noch unter ~/data/shopify/raw_chunks/. Dieses Skript spielt sie ein,
# OHNE die Shopify-API erneut zu befragen (der Abruf dauert ~3 Stunden).
#
# AUFRUF:
#   cd ~/git/dashboard
#   Rscript ~/git/dieseoR/scripts/produktiv/reingest_shopify_chunks.R 20260811_071537
#
# Ohne Run-ID wird der juengste vollstaendige Run automatisch gewaehlt.
#
# UNTERSCHIED ZUM ORIGINAL: Schlaegt etwas fehl, bleibt die Schatten-DB liegen
# (das Original loescht sie per file.remove und wirft damit die fertige Arbeit
# weg). Ausserdem wandert der alte Master vor dem Swap nach ~/.Trash/, ist also
# zurueckholbar.
# ==============================================================================

suppressMessages({
  library(dieseoR)
  library(DBI)
  library(duckdb)
})

reingest_shopify_chunks <- function(run_id = NULL,
                                    endpoint = "orders",
                                    datadir = "~/data/",
                                    keep_backup = TRUE) {
  raw_dir <- path.expand(file.path(datadir, "shopify", "raw_chunks"))
  db_path <- path.expand(file.path(datadir, "shopify", "shopify.duckdb"))
  update_path <- path.expand(file.path(datadir, "shopify", "shopify_update.duckdb"))

  stopifnot(dir.exists(raw_dir), file.exists(db_path))

  # --- 1. Run-ID bestimmen --------------------------------------------------
  if (is.null(run_id)) {
    all_chunks <- list.files(raw_dir, pattern = sprintf("^shopify_%s_.*_chunk_", endpoint))
    if (!length(all_chunks)) stop("Keine Roh-Chunks fuer Endpunkt '", endpoint, "' gefunden.")
    run_id <- sub(sprintf("^shopify_%s_(\\d{8}_\\d{6})_chunk_.*$", endpoint), "\\1", all_chunks) |>
      unique() |>
      sort() |>
      tail(1)
    message("ℹ️  Keine Run-ID uebergeben -- nutze juengsten Run: ", run_id)
  }

  files <- list.files(raw_dir,
    pattern = sprintf("^shopify_%s_%s_chunk_", endpoint, run_id),
    full.names = TRUE
  )
  if (!length(files)) stop("Keine Chunks fuer Run '", run_id, "' gefunden.")
  message(sprintf("📦 %d Chunk-Datei(en) fuer Run %s gefunden.", length(files), run_id))

  # --- 2. Einlesen & bereinigen --------------------------------------------
  raw <- lapply(files, readRDS) |> dplyr::bind_rows()
  message(sprintf("   -> %s Roh-Datensaetze eingelesen.", format(nrow(raw), big.mark = ".")))

  message("🧹 Bereinige mit clean_up_shopify()...")
  df_clean <- clean_up_shopify(shopify_data = raw, endpoint = endpoint)
  rm(raw)
  gc()
  message(sprintf(
    "   -> %s bereinigte Zeilen, %d Spalten.",
    format(nrow(df_clean), big.mark = "."), ncol(df_clean)
  ))

  pk_cols <- switch(endpoint,
    "orders"    = c("order_id", "item_id"),
    "products"  = "variants_id",
    "customers" = "id",
    "checkouts" = c("order_id", "item_id"),
    "id"
  )

  # Guardrail: ein Delta mit kaputtem PK wuerde beim Upsert Duplikate erzeugen,
  # weil die DELETE-Klausel auf NULL nicht matcht.
  pk_na <- vapply(pk_cols, function(k) sum(is.na(df_clean[[k]])), numeric(1))
  if (any(pk_na > 0)) {
    stop(
      "Delta enthaelt NULLs im Primaerschluessel (",
      paste(sprintf("%s: %d", names(pk_na), pk_na), collapse = ", "),
      ") -- Abbruch, sonst entstehen Duplikate."
    )
  }

  # --- 3. Schatten-DB anlegen ----------------------------------------------
  if (file.exists(update_path)) {
    stop(
      "Es liegt bereits eine Schatten-DB: ", update_path,
      "\n  -> Erst pruefen und wegraeumen (mv ... ~/.Trash/), dann erneut starten."
    )
  }
  message("🔌 Erstelle Schatten-Datenbank (Blue-Green)...")
  if (!file.copy(from = db_path, to = update_path, overwrite = FALSE)) {
    stop("file.copy der Master-DB fehlgeschlagen -- Abbruch.")
  }

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = update_path, read_only = FALSE)
  ok <- FALSE

  before <- DBI::dbGetQuery(con, sprintf(
    "SELECT COUNT(*) AS n, MAX(updated_at) AS max_upd FROM %s", endpoint
  ))

  tryCatch(
    {
      message("💾 Fuehre Upsert in die Schatten-DB aus...")
      perform_duckdb_upsert(con, df_clean, endpoint, pk_cols)

      after <- DBI::dbGetQuery(con, sprintf(
        "SELECT COUNT(*) AS n, MAX(updated_at) AS max_upd FROM %s", endpoint
      ))
      dupes <- DBI::dbGetQuery(con, sprintf(
        "SELECT COUNT(*) AS n FROM (SELECT %s FROM %s GROUP BY %s HAVING COUNT(*) > 1)",
        paste(pk_cols, collapse = ", "), endpoint, paste(pk_cols, collapse = ", ")
      ))$n

      message(sprintf(
        "   Vorher:  %s Zeilen, Stand %s",
        format(before$n, big.mark = "."), before$max_upd
      ))
      message(sprintf(
        "   Nachher: %s Zeilen, Stand %s",
        format(after$n, big.mark = "."), after$max_upd
      ))
      message(sprintf("   Duplikate auf PK: %d", dupes))

      # --- 4. Verifikation vor dem Swap --------------------------------------
      if (dupes > 0) stop("Duplikate auf dem Primaerschluessel entstanden.")
      if (after$n < before$n) stop("Zeilenzahl ist GESUNKEN -- Datenverlust, kein Swap.")
      if (!is.na(before$max_upd) && !is.na(after$max_upd) &&
        as.POSIXct(after$max_upd) < as.POSIXct(before$max_upd)) {
        stop("max(updated_at) ist zurueckgelaufen -- kein Swap.")
      }
      ok <- TRUE
    },
    error = function(e) {
      message("❌ Upsert/Verifikation fehlgeschlagen: ", conditionMessage(e))
    }
  )

  DBI::dbDisconnect(con, shutdown = TRUE)
  message("🔌 Verbindung zur Schatten-DB geschlossen.")

  # --- 5. Swap --------------------------------------------------------------
  if (!ok) {
    message("⚠️  Kein Swap. Die Schatten-DB bleibt zur Analyse liegen:\n     ", update_path)
    message("     Live-Master ist unveraendert.")
    return(invisible(FALSE))
  }

  if (keep_backup) {
    stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
    backup <- path.expand(sprintf("~/.Trash/shopify_master_vor_reingest_%s.duckdb", stamp))
    if (!file.rename(db_path, backup)) {
      stop("Konnte alten Master nicht nach ~/.Trash verschieben -- Abbruch vor dem Swap.")
    }
    message("🗄️  Alter Master gesichert: ", backup)
  }

  if (!file.rename(from = update_path, to = db_path)) {
    stop(
      "SWAP FEHLGESCHLAGEN. Schatten-DB liegt unter: ", update_path,
      if (keep_backup) paste0("\n  Alter Master: ", backup) else ""
    )
  }
  message("🔄 Atomarer Swap erfolgreich -- Master ist aktuell.")
  invisible(TRUE)
}

# --- Ausfuehrung (nur bei direktem Rscript-Aufruf) ---------------------------
if (sys.nframe() == 0L) {
  args <- commandArgs(trailingOnly = TRUE)
  rid <- if (length(args) >= 1) args[1] else NULL
  ep <- if (length(args) >= 2) args[2] else "orders"
  res <- reingest_shopify_chunks(run_id = rid, endpoint = ep)
  if (!isTRUE(res)) quit(status = 1)
}
