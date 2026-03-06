# schmierblatt

#' Lade alle Tickets aus Zendesk herunter und speichere sie lokal
#'
#' Diese Funktion verbindet sich mit der Zendesk API und lädt mittels
#' Cursor-Pagination alle verfügbaren Tickets herunter. Wenn bereits eine
#' Datei existiert, werden alte (gelöschte) Tickets behalten und bestehende aktualisiert.
#'
#' @param subdomain Character. Die Subdomain deines Zendesk-Kontos (z. B. "feew").
#' @param email Character. Die E-Mail-Adresse, mit der du dich bei Zendesk anmeldest.
#' @param api_token Character. Dein Zendesk API-Token.
#' @param save Logical. Gibt an, ob die Daten als .rds-Datei gespeichert werden sollen. Standard ist TRUE.
#' @param filename Character. Der Name der Datei (inkl. .rds Endung). Standard ist "all_tickets.rds".
#' @param merge_existing Logical. Wenn TRUE, werden neu geladene Daten mit der bestehenden lokalen Datei gemischt (Upsert), um in Zendesk gelöschte Tickets zu behalten.
#'
#' @return Ein Data Frame (Tibble) mit allen Zendesk-Tickets.
#' @export
#'
#' @importFrom httr GET authenticate status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr bind_rows distinct
#'
#' @examples
#' \dontrun{
#' tickets <- get_zendesk_tickets(
#'   subdomain = "feew",
#'   email = "muster@ffe3.de",
#'   api_token = "dein_geheimer_token"
#' )
#' }
get_zendesk_tickets <- function(subdomain, email, api_token, save = TRUE, filename = "all_tickets.rds", merge_existing = TRUE) {

  # 1. Lokale Pfade laden
  if (file.exists("~/workspace/local.R")) {
    source("~/workspace/local.R", local = TRUE)
  } else {
    stop("Die Datei '~/workspace/local.R' wurde nicht gefunden. Bitte anlegen, damit 'datadir' definiert ist.")
  }

  # 2. Authentifizierung vorbereiten
  user_auth <- paste0(email, "/token")
  url <- paste0("https://", subdomain, ".zendesk.com/api/v2/tickets.json?page[size]=100")

  # 3. Vorbereitungen für die Schleife
  all_tickets_list <- list()
  page_counter <- 1
  has_more <- TRUE

  # 4. Die Schleife
  while (has_more) {
    message("Lade Cursor-Seite ", page_counter, " ...")

    # NEU: Der Schutzpanzer gegen Verbindungsabbrüche
    response <- httr::RETRY(
      verb = "GET",
      url = url,
      config = httr::authenticate(user_auth, api_token),
      times = 5,        # Probiere es bis zu 5 Mal
      pause_base = 5,   # Warte 5 Sekunden zwischen den Versuchen
      quiet = FALSE     # Zeigt in der Konsole an, wenn er einen neuen Versuch startet
    )

    if (httr::status_code(response) == 200) {
      raw_content <- httr::content(response, "text", encoding = "UTF-8")
      parsed_data <- jsonlite::fromJSON(raw_content, flatten = TRUE)

      all_tickets_list[[page_counter]] <- parsed_data$tickets
      has_more <- parsed_data$meta$has_more
      url <- parsed_data$links[["next"]]

      page_counter <- page_counter + 1
      Sys.sleep(1)

    } else {
      stop("Fehler auf Seite ", page_counter, " - Statuscode: ", httr::status_code(response))
    }
  }

  # 5. Neue Daten zusammenfügen
  new_tickets_df <- dplyr::bind_rows(all_tickets_list)
  message("Download abgeschlossen. Neue/Aktuelle Tickets geladen: ", nrow(new_tickets_df))

  # 6. Speichern und Zusammenführen (Merge/Upsert)
  if (save) {
    if (!exists("datadir")) {
      stop("Die Variable 'datadir' wurde in ~/workspace/local.R nicht gefunden.")
    }

    zendesk_dir <- file.path(datadir, "zendesk")
    if (!dir.exists(zendesk_dir)) {
      dir.create(zendesk_dir, recursive = TRUE)
    }

    file_path <- file.path(zendesk_dir, filename)

    # --- DER MAGISCHE MERGE-PART ---
    if (merge_existing && file.exists(file_path)) {
      message("Bestehende Datei gefunden. Führe Update durch und bewahre gelöschte Tickets...")

      old_tickets_df <- readRDS(file_path)

      # Neue Daten ZUERST, alte Daten danach. Dann Duplikate anhand der ID löschen.
      # Dadurch gewinnt immer die neue Zeile, aber alte/gelöschte IDs bleiben am Ende hängen.
      final_tickets_df <- dplyr::bind_rows(new_tickets_df, old_tickets_df) |>
        dplyr::distinct(id, .keep_all = TRUE)

      message("Merge erfolgreich. Gesamtanzahl Tickets jetzt: ", nrow(final_tickets_df))
    } else {
      # Falls noch keine alte Datei da ist oder merge_existing = FALSE
      final_tickets_df <- new_tickets_df
    }
    # -------------------------------

    saveRDS(final_tickets_df, file = file_path)
    message("Daten erfolgreich gespeichert unter: ", file_path)

    return(final_tickets_df)

  } else {
    # Wenn nicht gespeichert werden soll, gib einfach die neu geladenen zurück
    return(new_tickets_df)
  }
}
