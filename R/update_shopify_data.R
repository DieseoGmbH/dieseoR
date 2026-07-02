#' @title Inkrementelles Update der Shopify Daten (DuckDB Upsert)
#'
#' @description Zieht geaenderte Daten via API, entpackt diese dynamisch je nach
#' Endpunkt und fuehrt einen In-Database Upsert durch. Memory-Safe.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param endpoint Character. Welcher Endpunkt aktualisiert werden soll ("orders", "checkouts", "products", "customers").
#' @param api_key Character. Das Shopify Admin API Access Token.
#'
#' @return Invisible TRUE bei Erfolg.
#' @export
#'
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery dbExecute dbWriteTable dbExistsTable
#' @importFrom duckdb duckdb
#' @importFrom lubridate days
#' @importFrom dplyr bind_rows
update_shopify_data <- function(datadir = "~/data",
                                endpoint = "orders",
                                api_key) {
  if (missing(api_key) || api_key == "") {
    stop("Fehler: api_key muss uebergeben werden.", call. = FALSE)
  }

  # 1. Datenbank-Pfad definieren
  db_path <- file.path(datadir, "shopify", "shopify.duckdb")

  if (!file.exists(db_path)) {
    stop(sprintf("Keine DuckDB unter '%s' gefunden. Bitte fuehre zuerst den Backfill aus.", db_path), call. = FALSE)
  }

  message(sprintf("\n🔌 Verbinde mit DuckDB für Update: %s", db_path))
  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path)

  # 🚨 WICHTIG: Stellt sicher, dass die Verbindung JEDERZEIT geschlossen wird (auch bei Errors!)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # 2. Startdatum für den API-Call direkt über SQL ermitteln
  message(sprintf("Lese maximales 'updated_at' aus der Tabelle '%s'...", endpoint))

  if (!DBI::dbExistsTable(con, endpoint)) {
    stop(sprintf("Tabelle '%s' existiert nicht in der Datenbank.", endpoint), call. = FALSE)
  }

  query_max_date <- sprintf("SELECT MAX(updated_at) AS max_date FROM %s", endpoint)
  max_date_res <- DBI::dbGetQuery(con, query_max_date)

  if (nrow(max_date_res) > 0 && !is.na(max_date_res$max_date)) {
    max_date <- as.Date(max_date_res$max_date)
    # 100 Tage zurückblicken, um späte Stornos/Refunds sicher abzugreifen
    start_fetch <- max_date - lubridate::days(50)
  } else {
    message("Konnte kein 'updated_at' finden. Lade die letzten 100 Tage als Fallback.")
    start_fetch <- Sys.Date() - lubridate::days(50)
  }

  updated_at_min <- format(start_fetch, "%Y-%m-%dT00:00:00Z")
  message(sprintf("Starte inkrementelles Update für '%s' ab %s...", endpoint, updated_at_min))

  # 3. Neue Daten von der API ziehen
  df_new_raw <- get_shopify_data(
    api_key = api_key,
    endpoint = endpoint,
    updated_at_min = updated_at_min
  )

  if (nrow(df_new_raw) == 0) {
    message("✅ Keine neuen oder geupdateten Daten gefunden. Datenbank ist aktuell.")
    return(invisible(TRUE))
  }

  message(sprintf("📦 %s veränderte Datensaetze geladen. Starte speicherschonende Bereinigung...", nrow(df_new_raw)))

  # ----------------------------------------------------------------------------
  # 4. MEMORY-SAFE BEREINIGUNG (CHUNK-BASED)
  # ----------------------------------------------------------------------------
  chunk_size <- 2500
  n_rows <- nrow(df_new_raw)

  if (n_rows > chunk_size) {
    raw_chunks <- split(df_new_raw, ceiling(seq_len(n_rows) / chunk_size))
    cleaned_chunks <- list()

    for (i in seq_along(raw_chunks)) {
      message(sprintf("  -> Bereinige Chunk %d von %d...", i, length(raw_chunks)))
      cleaned_chunks[[i]] <- clean_up_shopify(raw_chunks[[i]], endpoint = endpoint)

      raw_chunks[[i]] <- NULL
      gc()
    }

    df_new_clean <- dplyr::bind_rows(cleaned_chunks)
    rm(cleaned_chunks)
    gc()
  } else {
    df_new_clean <- clean_up_shopify(df_new_raw, endpoint = endpoint)
  }

  rm(df_new_raw)
  gc()

  # ----------------------------------------------------------------------------
  # 5. IN-DATABASE UPSERT (Append + SQL Deduplizierung)
  # ----------------------------------------------------------------------------
  message(sprintf("\n💾 Schreibe %d bereinigte Zeilen in die DuckDB-Tabelle '%s'...", nrow(df_new_clean), endpoint))

  # A. Neue Daten direkt anhängen
  DBI::dbWriteTable(con, endpoint, df_new_clean, append = TRUE)

  rm(df_new_clean)
  gc()

  message("🛠️ Führe Deduplizierung direkt in der Datenbank aus (Out-of-Core)...")

  # B. Dynamische Partition Keys je nach Endpunkt
  partition_keys <- switch(endpoint,
    "orders"    = "order_id, item_id",
    "products"  = "variants_id",
    "customers" = "id",
    "checkouts" = "order_id, item_id",
    "id" # Fallback
  )

  # C. SQL Window-Funktion zur Bereinigung anwenden
  dedup_query <- sprintf("
    CREATE OR REPLACE TABLE %s AS
    SELECT * EXCLUDE (rn)
    FROM (
      SELECT *,
             ROW_NUMBER() OVER(
               PARTITION BY %s
               ORDER BY updated_at DESC NULLS LAST
             ) as rn
      FROM %s
    )
    WHERE rn = 1;
  ", endpoint, partition_keys, endpoint)

  DBI::dbExecute(con, dedup_query)

  # D. Datenbank-Datei aufräumen
  DBI::dbExecute(con, "VACUUM;")

  final_rows <- DBI::dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", endpoint))$n

  message(sprintf("✅ Upsert erfolgreich! Die Tabelle '%s' hat nun %d bereinigte und eindeutige Zeilen.", endpoint, final_rows))

  return(invisible(TRUE))
}
