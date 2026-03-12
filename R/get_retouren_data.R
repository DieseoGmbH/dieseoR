#' Lade alle Retouren aus der Musterportal API herunter und speichere sie lokal
#'
#' Diese Funktion verbindet sich mit der Retouren-API und lädt mittels
#' Pagination alle verfügbaren Retouren herunter. Wenn bereits eine
#' Datei existiert, werden alte Retouren behalten und bestehende aktualisiert (Upsert).
#' Ein automatisches Rate-Limiting (1 Sekunde Pause pro Seite) verhindert 429-Fehler.
#'
#' @param api_key Character. Dein N8N-API-KEY.
#' @param base_url Character. Die Basis-URL der API. Standard ist "https://musterportal.com/api/all-returns".
#' @param save Logical. Gibt an, ob die Daten als .rds-Datei gespeichert werden sollen. Standard ist TRUE.
#' @param filename Character. Der Name der Datei (inkl. .rds Endung). Standard ist "all_returns.rds".
#' @param merge_existing Logical. Wenn TRUE, werden neu geladene Daten mit der bestehenden lokalen Datei gemischt (Upsert).
#'
#' @return Ein Data Frame (Tibble) mit allen Retouren.
#' @export
#'
#' @importFrom httr RETRY add_headers status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr bind_rows distinct
#'
#' @examples
#' \dontrun{
#' returns <- get_retouren_data(
#'   api_key = "dein_geheimer_token"
#' )
#' }
get_retouren_data <- function(api_key,
                              base_url = "https://musterportal.com/api/all-returns",
                              save = TRUE,
                              filename = "all_returns.rds",
                              merge_existing = TRUE) {
  # 1. Lokale Pfade laden (für datadir)
  if (file.exists("~/workspace/local.R")) {
    source("~/workspace/local.R", local = TRUE)
  } else {
    stop("Die Datei '~/workspace/local.R' wurde nicht gefunden. Bitte anlegen, damit 'datadir' definiert ist.")
  }

  # 2. Vorbereitungen für die Schleife
  all_returns_list <- list()
  page <- 1
  keep_going <- TRUE

  message("Starte den Download aller Retouren (mit Tempolimit)...")

  # 3. Die Schleife
  while (keep_going) {
    # API aufrufen mit RETRY und sauberer query-Übergabe
    response <- httr::RETRY(
      verb = "GET",
      url = base_url,
      query = list(per_page = 100, page = page),
      config = httr::add_headers(`N8N-API-KEY` = api_key),
      times = 5, # Probiere es bis zu 5 Mal bei Fehlern
      pause_base = 3, # Warte 3 Sekunden zwischen den Fehler-Versuchen
      quiet = TRUE
    )

    # Fehlerbehandlung
    if (httr::status_code(response) != 200) {
      message("Fehler auf Seite ", page, ". Status-Code: ", httr::status_code(response))
      message("Das sagt der Server:")
      httr::content(response, "text", encoding = "UTF-8")
      stop("Download abgebrochen wegen API-Fehler.")
    }

    # Daten entpacken (Wir springen in den doppelten data-Ordner!)
    raw_content <- httr::content(response, "text", encoding = "UTF-8")
    parsed_data <- jsonlite::fromJSON(raw_content, flatten = TRUE)
    current_data <- parsed_data$data$data

    # Abbruchbedingung prüfen
    if (is.null(current_data) || length(current_data) == 0 || nrow(current_data) == 0) {
      message("Keine weiteren Daten auf Seite ", page, ". Download beendet!")
      keep_going <- FALSE
    } else {
      all_returns_list[[page]] <- current_data
      message("Lade Cursor-Seite ", page, " (", nrow(current_data), " Zeilen) ...")

      page <- page + 1

      # Die Rate-Limit-Bremse
      Sys.sleep(1)
    }
  }

  # 4. Neue Daten zusammenfügen
  if (length(all_returns_list) == 0) {
    message("Es wurden gar keine Daten gefunden.")
    return(NULL)
  }

  new_returns_df <- dplyr::bind_rows(all_returns_list)
  message("Download abgeschlossen. Neue/Aktuelle Retouren geladen: ", nrow(new_returns_df))

  # 5. Speichern und Zusammenführen (Merge/Upsert)
  if (save) {
    if (!exists("datadir")) {
      stop("Die Variable 'datadir' wurde in ~/workspace/local.R nicht gefunden.")
    }

    # Neuen Unterordner für Retouren anlegen
    returns_dir <- file.path(datadir, "returns")
    if (!dir.exists(returns_dir)) {
      dir.create(returns_dir, recursive = TRUE)
    }

    file_path <- file.path(returns_dir, filename)

    # --- DER MAGISCHE MERGE-PART ---
    if (merge_existing && file.exists(file_path)) {
      message("Bestehende Datei gefunden. Führe Update durch...")

      old_returns_df <- readRDS(file_path)

      # Neue Daten ZUERST, alte Daten danach. Duplikate anhand der 'id' löschen.
      final_returns_df <- dplyr::bind_rows(new_returns_df, old_returns_df) |>
        dplyr::distinct(id, .keep_all = TRUE)

      message("Merge erfolgreich. Gesamtanzahl Retouren jetzt: ", nrow(final_returns_df))
    } else {
      final_returns_df <- new_returns_df
    }
    # -------------------------------

    saveRDS(final_returns_df, file = file_path)
    message("Daten erfolgreich gespeichert unter: ", file_path)

    final_returns_df
  } else {
    new_returns_df
  }
}
