# ==============================================================================
# SCRIPT: R/update_shopify_data.R
# PACKAGE: dieseoR
# ==============================================================================

#' @title Get Latest Shopify Timestamp from DuckDB
#'
#' @description Ermittelt den aktuellsten `updated_at` Zeitstempel fuer einen
#' spezifischen Shopify-Endpunkt aus der DuckDB. Zieht einen konfigurierbaren
#' Puffer ab, um "Late-Arriving" Updates sicher zu erfassen.
#'
#' @param con DuckDB Connection Object.
#' @param endpoint Character. Der Name der Tabelle (z.B. "orders").
#' @param buffer_days Integer. Anzahl der Tage, die als Sicherheitspuffer abgezogen werden (Standard: 7).
#'
#' @return Character (ISO 8601 Timestamp) oder NULL, falls die Tabelle nicht existiert/leer ist.
#' @importFrom DBI dbExistsTable dbGetQuery
#' @importFrom lubridate days
#' @export
get_latest_shopify_timestamp <- function(con, endpoint, buffer_days = 7) {
  if (!DBI::dbExistsTable(con, endpoint)) {
    message(sprintf("ℹ️ Tabelle '%s' existiert noch nicht in DuckDB.", endpoint))
    return(NULL)
  }

  query <- sprintf("SELECT MAX(updated_at) as max_date FROM %s", endpoint)
  max_date <- DBI::dbGetQuery(con, query)$max_date

  if (is.na(max_date)) {
    return(NULL)
  }

  # Parse und Puffer abziehen
  parsed_date <- as.POSIXct(max_date, tz = "UTC")
  delta_start <- parsed_date - lubridate::days(buffer_days)

  # Shopify erwartet das Format ISO 8601
  format(delta_start, "%Y-%m-%dT%H:%M:%S%z")
}

#' @title Perform RAM-Safe DuckDB Upsert with Schema Evolution
#'
#' @description Schreibt Delta-Daten in eine Staging-Tabelle, prueft auf neue
#' Spalten (Schema Evolution), loescht veraltete Datensaetze in der Haupttabelle
#' (Update-Logik) und fuegt die neuen Daten sicher per 'BY NAME' ein.
#'
#' @param con DuckDB Connection Object.
#' @param df_delta Data.frame/Tibble. Die bereinigten neuen/geaenderten Daten (Staging).
#' @param endpoint Character. Ziel-Tabelle (z.B. "orders").
#' @param pk_cols Character Vector. Spaltennamen, die den Primary Key bilden.
#'
#' @return Invisible TRUE bei Erfolg.
#' @importFrom DBI dbWriteTable dbListFields dbExecute dbRemoveTable
#' @export
perform_duckdb_upsert <- function(con, df_delta, endpoint, pk_cols) {
  staging_table <- paste0(endpoint, "_staging")

  # 1. Delta in temporaere Staging-Tabelle schreiben
  DBI::dbWriteTable(con, staging_table, df_delta, overwrite = TRUE)

  # 2. Schema Evolution pruefen: Gibt es neue Spalten im Delta?
  existing_cols <- DBI::dbListFields(con, endpoint)
  new_cols <- setdiff(names(df_delta), existing_cols)

  if (length(new_cols) > 0) {
    message(sprintf("⚠️ Schema-Evolution: Fuege %d neue Spalten zu '%s' hinzu...", length(new_cols), endpoint))

    for (col in new_cols) {
      # R-Typ auf SQL-Typ mappen
      r_type <- class(df_delta[[col]])[1]
      sql_type <- switch(r_type,
        "integer" = "INTEGER",
        "numeric" = "DOUBLE",
        "logical" = "BOOLEAN",
        "POSIXct" = "TIMESTAMP",
        "Date" = "DATE",
        "character" = "VARCHAR",
        {
          # Fallback fuer unbekannte Datentypen
          message(sprintf("  ⚠️ Unbekannter Datentyp '%s' bei Spalte '%s'. Nutze Fallback 'VARCHAR'.", r_type, col))
          "VARCHAR"
        }
      )

      # Tabelle dynamisch erweitern (Historie wird mit NULL befuellt)
      alter_query <- sprintf('ALTER TABLE %s ADD COLUMN "%s" %s', endpoint, col, sql_type)
      DBI::dbExecute(con, alter_query)
      message(sprintf("  -> Spalte '%s' (%s) hinzugefuegt.", col, sql_type))
    }
  }

  # 3. Upsert Logik: Loesche veraltete Zeilen aus der Haupttabelle
  pk_str <- paste(pk_cols, collapse = ", ")
  delete_query <- sprintf("
    DELETE FROM %s
    WHERE (%s) IN (SELECT %s FROM %s)
  ", endpoint, pk_str, pk_str, staging_table)

  DBI::dbExecute(con, delete_query)

  # 4. Neue Daten einfuegen (BY NAME faengt fehlende Spalten automatisch ab)
  insert_query <- sprintf("
    INSERT INTO %s BY NAME
    SELECT * FROM %s
  ", endpoint, staging_table)

  DBI::dbExecute(con, insert_query)

  # 5. Aufraeumen
  DBI::dbRemoveTable(con, staging_table)

  invisible(TRUE)
}

#' @title Inkrementelles Update der Shopify Daten (DuckDB Blue-Green Swap)
#'
#' @description Orchestriert den Delta-Ladevorgang fuer Shopify per Blue-Green Deployment.
#' Kopiert die Live-DuckDB in eine Schatten-Datenbank, fuehrt den Upsert isoliert durch und
#' tauscht die DB-Datei am Ende atomar aus. Das verhindert read-only Concurrency-Errors.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param endpoint Character. Welcher Endpunkt aktualisiert werden soll ("orders", "products" etc.).
#' @param api_key Character. Das Shopify Admin API Access Token.
#' @param buffer_days Integer. Zeitpuffer in Tagen fuer "Late-Arriving" Updates.
#'
#' @return Invisible TRUE bei Erfolg, FALSE bei Fehler.
#' @importFrom DBI dbConnect dbDisconnect dbIsValid
#' @importFrom duckdb duckdb
#' @export
update_shopify_data <- function(datadir = "~/data",
                                endpoint = "orders",
                                api_key,
                                buffer_days = 7) {
  if (missing(api_key) || api_key == "") {
    stop("Fehler: api_key muss uebergeben werden.")
  }

  # Architektur-Pfade definieren
  db_path <- file.path(datadir, "shopify", "shopify.duckdb")
  update_path <- file.path(datadir, "shopify", "shopify_update.duckdb")
  raw_dir <- file.path(datadir, "shopify", "raw_chunks")

  if (!dir.exists(raw_dir)) dir.create(raw_dir, recursive = TRUE)

  message(sprintf("\n🔌 Bereite Blue-Green Swap für Shopify Endpunkt '%s' vor...", toupper(endpoint)))

  # 1. Schattenkopie erstellen
  if (file.exists(db_path)) {
    message("  -> Erstelle Schatten-Datenbank. Dashboard-Lesezugriff bleibt ungestört.")
    file.copy(from = db_path, to = update_path, overwrite = TRUE)
  } else {
    message("  -> Keine bestehende DuckDB gefunden. Initialer Lauf...")
  }

  con <- NULL
  update_success <- FALSE

  # Robustes Error-Handling
  tryCatch({
    # 2. Verbinde EXPLIZIT mit der Schatten-DB im Write-Modus
    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = update_path, read_only = FALSE)

    # 3. Dynamischen Timestamp holen (aus der Kopie)
    last_sync <- get_latest_shopify_timestamp(con, endpoint, buffer_days)

    if (is.null(last_sync)) {
      message("ℹ️ Kein bestehender Timestamp. Starte Abruf ohne Datumsfilter.")
    } else {
      message(sprintf("⏳ Letzter Sync (inkl. %d Tage Puffer): %s", buffer_days, last_sync))
    }

    # 4. Delta-Daten von der API laden
    message(sprintf("📥 Ziehe Shopify-Deltas fuer Endpunkt '%s'...", endpoint))

    df_delta <- get_shopify_data(
      shop_url = "pummmys.myshopify.com",
      api_key = api_key,
      endpoint = endpoint,
      raw_dir = raw_dir,
      updated_at_min = last_sync
    )

    if (is.null(df_delta) || nrow(df_delta) == 0) {
      message("✅ Keine neuen Daten bei Shopify gefunden. Update uebersprungen.")
      update_success <- TRUE
      return(invisible(TRUE)) # Beendet den TryCatch-Block erfolgreich
    }

    message(sprintf("🔄 %d neue/geaenderte Datensaetze gefunden. Starte Bereinigung...", nrow(df_delta)))

    # 5. Datenflachung und Formatierung
    df_clean <- clean_up_shopify(shopify_data = df_delta, endpoint = endpoint)

    # RAM-Optimierung
    rm(df_delta)
    gc()

    # 6. Primary Key dynamisch bestimmen
    pk_cols <- switch(endpoint,
      "orders"    = c("order_id", "item_id"),
      "products"  = "variants_id",
      "customers" = "id",
      "checkouts" = c("order_id", "item_id"),
      "id" # Fallback
    )

    # 7. Upsert in die KOPIE durchfuehren
    message("💾 Fuehre datenbankbasierten Upsert (inkl. Schema-Check) in Schatten-DB aus...")
    perform_duckdb_upsert(con, df_clean, endpoint, pk_cols)

    rm(df_clean)
    gc()

    message("✅ Shopify Deltas erfolgreich in Schatten-DB integriert.")
    update_success <- TRUE
  }, error = function(e) {
    message("❌ FEHLER beim Shopify Update: ", e$message)
  }, finally = {
    # 8. Verbindung garantiert schliessen, um File-Locks zu loesen
    if (!is.null(con) && DBI::dbIsValid(con)) {
      DBI::dbDisconnect(con, shutdown = TRUE)
      message("🔌 Verbindung zur Schatten-DB geschlossen.")
    }

    # 9. DER ATOMARE SWAP (Nur wenn alles erfolgreich war!)
    if (update_success && file.exists(update_path)) {
      # file.rename() ueberschreibt die Live-DB atomar
      file.rename(from = update_path, to = db_path)
      message("🔄 Atomarer Swap erfolgreich! Das Dashboard nutzt nun die aktuellsten Daten.")
    } else {
      message("⚠️ Update fehlgeschlagen oder abgebrochen. Live-DB bleibt unangetastet.")
      if (file.exists(update_path)) {
        file.remove(update_path) # Schattenkopie sicher loeschen
      }
    }
  })

  invisible(update_success)
}
