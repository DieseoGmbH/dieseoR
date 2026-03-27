#' Upload data to Google BigQuery
#'
#' @title Upload Data to BigQuery
#' @description Authentifiziert sich via JSON-Service-Account-Key, extrahiert automatisch die Projekt-ID und lädt ein Dataframe in eine BigQuery Tabelle hoch.
#' @param data Ein data.frame oder tibble, das hochgeladen werden soll.
#' @param dataset_id Character. Die BigQuery Dataset-ID.
#' @param table_id Character. Der Name der Zieltabelle (wird erstellt, falls nicht existent).
#' @param json_key_path Character. Der lokale Pfad zur .json Schlüsseldatei.
#' @param write_disposition Character. Verhalten, wenn die Tabelle schon existiert. Standard ist "WRITE_APPEND". Alternativ: "WRITE_TRUNCATE".
#' @return Invisible TRUE bei erfolgreichem Upload.
#' @export
#' @importFrom bigrquery bq_auth bq_table bq_table_upload
#' @importFrom jsonlite fromJSON
#' @examples \dontrun{
#' upload_to_bigquery(
#'   data = clean_zendesk_data,
#'   table_id = "zendesk_tickets"
#' )
#' }
upload_to_bigquery <- function(data,
                               dataset_id = "support_analytics",
                               table_id,
                               json_key_path = "/Users/andres/git/dieseoR/scripts/data-analytics-491117-58f718e1ee61.json",
                               write_disposition = "WRITE_APPEND") {
  # 1. Error Handling: Existiert der Key?
  if (!file.exists(json_key_path)) {
    stop("Die JSON-Schlüsseldatei wurde unter dem angegebenen Pfad nicht gefunden: ", json_key_path)
  }

  # 2. Projekt-ID automatisch aus JSON extrahieren
  project_id <- tryCatch(
    {
      key_data <- jsonlite::fromJSON(json_key_path)
      if (is.null(key_data$project_id)) stop("Keine 'project_id' im JSON gefunden.")
      key_data$project_id
    },
    error = function(e) {
      stop("Fehler beim Auslesen der project_id aus dem JSON-Key: ", e$message)
    }
  )

  # 3. Authentifizierung
  tryCatch(
    {
      bigrquery::bq_auth(path = json_key_path)
      message("Erfolgreich bei Google Cloud (Projekt: ", project_id, ") authentifiziert.")
    },
    error = function(e) {
      stop("Authentifizierung fehlgeschlagen. Bitte JSON-Key prüfen. Fehler: ", e$message)
    }
  )

  # 4. Referenz zur Zieltabelle erstellen
  tb <- bigrquery::bq_table(
    project = project_id,
    dataset = dataset_id,
    table = table_id
  )

  # 5. Upload der Daten
  tryCatch(
    {
      message("Starte Daten-Upload nach BigQuery (", nrow(data), " Zeilen)...")

      bigrquery::bq_table_upload(
        x = tb,
        values = data,
        create_disposition = "CREATE_IF_NEEDED",
        write_disposition = write_disposition,
        billing = project_id # <--- Zur Sicherheit ergänzen
      )

      message("Upload erfolgreich abgeschlossen!")
    },
    error = function(e) {
      stop("Upload fehlgeschlagen: ", e$message)
    }
  )

  invisible(TRUE)
}
