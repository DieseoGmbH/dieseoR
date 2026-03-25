#' @title Inkrementelles Update der Trustpilot Daten (Upsert)
#'
#' @description Liest die bestehende bereinigte Trustpilot `.rds` Datei ein,
#' ermittelt das aktuellste Bewertungsdatum, zieht die neuesten Bewertungen
#' via API und fuehrt einen deduplizierten Upsert durch.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param data_path Character. Der direkte Dateipfad zur `.rds` Datei.
#' Überschreibt standardmäßig die Konstruktion aus `datadir`.
#' @param pages_to_fetch Numeric. Wie viele Seiten (à 100 Bewertungen) sollen
#' standardmäßig für das Update abgerufen werden? (Default: 5 Seiten = 500 neuste Reviews).
#'
#' @return Invisible TRUE bei Erfolg.
#' @export
#'
#' @importFrom dplyr bind_rows distinct filter
#'
#' @examples \dontrun{
#' # Standardaufruf (nutzt ~/data/trustpilot/...)
#' update_trustpilot_data(datadir = "~/data")
#'
#' # Spezifischer Aufruf aus dem Shiny-Verzeichnis
#' update_trustpilot_data(data_path = "data/cleaned_trustpilot_data.rds")
#' }
update_trustpilot_data <- function(datadir = "~/data",
                                   data_path = file.path(datadir, "trustpilot", "cleaned_trustpilot_data.rds"),
                                   pages_to_fetch = 5) {
  # 1. Pruefen, ob die Historie ueberhaupt existiert
  if (!file.exists(data_path)) {
    stop(sprintf("Keine Master-RDS unter '%s' gefunden. Bitte fuehre zuerst den Initial Load aus.", data_path), call. = FALSE)
  }

  message(sprintf("Lese bestehende Trustpilot-Datenbank ein aus: %s", data_path))
  df_existing <- readRDS(data_path)

  # 2. Startdatum fuer den Abgleich ermitteln
  if ("created_at" %in% names(df_existing)) {
    last_date <- max(df_existing$created_at, na.rm = TRUE)
  } else {
    message("Konnte kein Datumsfeld 'created_at' finden. Nutze Fallback-Datum.")
    last_date <- Sys.time() - (7 * 24 * 60 * 60) # 7 Tage zurueck
  }

  message(sprintf("Letzter Stand der Trustpilot-Daten: %s", last_date))
  message("Ziehe neue Daten von der API...")

  # 3. Neue Daten ziehen (wir holen die neuesten Seiten)
  df_new_raw <- get_trustpilot_data(max_pages = pages_to_fetch)

  if (nrow(df_new_raw) == 0) {
    message("Keine Daten von der API zurueckgegeben.")
    return(invisible(TRUE))
  }

  # 4. Neue Daten bereinigen (Transformation)
  df_new_clean <- clean_up_trustpilot(df_new_raw)

  # Filtern auf wirkliche Neuheiten (alles was neuer ist als unser last_date)
  df_new_filtered <- df_new_clean |>
    dplyr::filter(created_at > last_date)

  if (nrow(df_new_filtered) == 0) {
    message("Keine neuen Trustpilot-Bewertungen gefunden. Datenbank ist aktuell.")
    return(invisible(TRUE))
  }

  # 5. Daten anhaengen und deduplizieren (Upsert-Logik)
  df_combined <- dplyr::bind_rows(df_existing, df_new_filtered)

  if ("review_id" %in% names(df_combined)) {
    df_combined <- df_combined |>
      dplyr::distinct(review_id, .keep_all = TRUE)
  } else {
    # Fallback
    df_combined <- dplyr::distinct(df_combined)
  }

  # 6. Datenbank ueberschreiben
  saveRDS(df_combined, data_path)
  message(sprintf(
    "Upsert erfolgreich! %s neue Datensaetze integriert. Gesamt: %s",
    nrow(df_new_filtered), nrow(df_combined)
  ))

  return(invisible(TRUE))
}
