#' @title Upload Data to BigQuery
#' @description Authentifiziert sich und lädt via ausfallsicherem Parquet-Format nach BigQuery hoch.
#' @importFrom bigrquery bq_auth bq_table bq_table_upload
#' @importFrom jsonlite fromJSON
#' @importFrom arrow write_parquet
#' @export
upload_to_bigquery <- function(data, dataset_id = "support_analytics", table_id, json_key_path = "~/git/dieseoR/scripts/data-analytics-491117-58f718e1ee61.json",
                               write_disposition = "WRITE_APPEND") {
  if (!file.exists(json_key_path)) {
    stop(
      "Die JSON-Schlüsseldatei wurde unter dem angegebenen Pfad nicht gefunden: ",
      json_key_path
    )
  }
  project_id <- tryCatch(
    {
      key_data <- jsonlite::fromJSON(json_key_path)
      if (is.null(key_data$project_id)) {
        stop("Keine 'project_id' im JSON gefunden.")
      }
      key_data$project_id
    },
    error = function(e) {
      stop("Fehler beim Auslesen der project_id: ", e$message)
    }
  )
  tryCatch(
    {
      bigrquery::bq_auth(path = json_key_path)
      message(
        "Erfolgreich bei Google Cloud (Projekt: ", project_id,
        ") authentifiziert."
      )
    },
    error = function(e) {
      stop("Authentifizierung fehlgeschlagen: ", e$message)
    }
  )
  tb <- bigrquery::bq_table(
    project = project_id, dataset = dataset_id,
    table = table_id
  )
  tryCatch(
    {
      message(
        "Starte Daten-Upload nach BigQuery (",
        nrow(data), " Zeilen)..."
      )

      # Zurück zum Standard: values MUSS das R-Dataframe ('data') sein
      bigrquery::bq_table_upload(
        x = tb,
        values = data,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = write_disposition,
        billing = project_id,
        schema_update_options = "ALLOW_FIELD_RELAXATION"
      )
      message("✅ Upload erfolgreich abgeschlossen!")
    },
    error = function(e) {
      stop("Upload fehlgeschlagen: ", e$message)
    }
  )
  invisible(TRUE)
}
