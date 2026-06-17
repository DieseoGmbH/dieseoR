#' @title Upload Data to BigQuery
#' @description Authentifiziert sich und lädt via ausfallsicherem Parquet-Format nach BigQuery hoch.
#' @importFrom bigrquery bq_auth bq_table bq_table_upload
#' @importFrom jsonlite fromJSON
#' @export
upload_to_bigquery <- function(data,
                               dataset_id = "support_analytics",
                               table_id,
                               json_key_path = "~/git/dieseoR/scripts/data-analytics-491117-58f718e1ee61.json",
                               write_disposition = "WRITE_APPEND") {
  if (!file.exists(json_key_path)) {
    stop("Die JSON-Schlüsseldatei wurde nicht gefunden: ", json_key_path)
  }

  # 1. Project ID auslesen
  project_id <- tryCatch(
    {
      key_data <- jsonlite::fromJSON(json_key_path)
      if (is.null(key_data$project_id)) stop("Keine 'project_id' im JSON gefunden.")
      key_data$project_id
    },
    error = function(e) stop("Fehler beim Auslesen der project_id: ", e$message)
  )

  # 2. Authentifizierung
  tryCatch(
    {
      bigrquery::bq_auth(path = json_key_path)
      message("Erfolgreich bei Google Cloud (Projekt: ", project_id, ") authentifiziert.")
    },
    error = function(e) stop("Authentifizierung fehlgeschlagen: ", e$message)
  )

  # 3. Tabellen-Referenz
  tb <- bigrquery::bq_table(project = project_id, dataset = dataset_id, table = table_id)

  # 4. Upload via nativem bigrquery Parquet-Support
  tryCatch(
    {
      message("Starte Daten-Upload nach BigQuery (", nrow(data), " Zeilen via Parquet)...")

      # 💡 LÖSUNG: Wir übergeben das Dataframe direkt, schalten aber auf PARQUET um!
      bigrquery::bq_table_upload(
        x = tb,
        values = data,
        source_format = "PARQUET",
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = write_disposition
      )

      message("✅ Upload erfolgreich abgeschlossen!")
    },
    error = function(e) {
      stop("Upload fehlgeschlagen: ", e$message)
    }
  )

  invisible(TRUE)
}
