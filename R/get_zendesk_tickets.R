#' Lade alle Tickets aus Zendesk herunter und speichere sie lokal
#'
#' Diese Funktion verbindet sich mit der Zendesk API und lädt mittels
#' Cursor-Pagination alle verfügbaren Tickets herunter. Die Daten werden
#' als Data Frame zurückgegeben. Optional können die Daten automatisch in
#' dem in `~/local.R` definierten `datadir` unter dem Unterordner "zendesk"
#' gespeichert werden.
#'
#' @param subdomain Character. Die Subdomain deines Zendesk-Kontos (z. B. "feew").
#' @param email Character. Die E-Mail-Adresse, mit der du dich bei Zendesk anmeldest.
#' @param api_token Character. Dein Zendesk API-Token.
#' @param save Logical. Gibt an, ob die Daten als .rds-Datei gespeichert werden sollen. Standard ist TRUE.
#' @param filename Character. Der Name der Datei (inkl. .rds Endung), wenn `save = TRUE`. Standard ist "all_tickets.rds".
#'
#' @return Ein Data Frame (Tibble) mit allen Zendesk-Tickets.
#' @export
#'
#' @importFrom httr GET authenticate status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr bind_rows
#'
#' @examples
#' \dontrun{
#' # Nur in R laden, ohne zu speichern:
#' tickets <- get_zendesk_tickets(
#'   subdomain = "feew",
#'   email = "muster@ffe3.de",
#'   api_token = "dein_geheimer_token",
#'   save = FALSE
#' )
#'
#' # Speichern mit eigenem Dateinamen:
#' tickets <- get_zendesk_tickets(
#'   subdomain = "feew",
#'   email = "muster@ffe3.de",
#'   api_token = "dein_geheimer_token",
#'   save = TRUE,
#'   filename = "tickets_backup_heute.rds"
#' )
#' }
get_zendesk_tickets <- function(subdomain, email, api_token, save = TRUE, filename = "all_tickets.rds") {

  # 1. Lokale Pfade laden
  if (file.exists("~/local.R")) {
    # local = TRUE sorgt dafür, dass datadir und syncdir nur in dieser
    # Umgebung existieren und nicht global überschreiben
    source("~/local.R", local = TRUE)
  } else {
    stop("Die Datei '~/local.R' wurde nicht gefunden. Bitte anlegen, damit 'datadir' definiert ist.")
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

    response <- httr::GET(url, httr::authenticate(user_auth, api_token))

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

  # 5. Daten zusammenfügen
  all_tickets_df <- dplyr::bind_rows(all_tickets_list)
  message("Fertig! Insgesamt geladene Tickets: ", nrow(all_tickets_df))

  # 6. Optionales Speichern (gesteuert durch Argument 'save')
  if (save) {
    if (!exists("datadir")) {
      stop("Die Variable 'datadir' wurde in ~/local.R nicht gefunden. Bitte dort eintragen oder 'save = FALSE' nutzen.")
    }

    # Zielordner erstellen (datadir + "zendesk")
    zendesk_dir <- file.path(datadir, "zendesk")
    if (!dir.exists(zendesk_dir)) {
      dir.create(zendesk_dir, recursive = TRUE)
    }

    # Datei speichern mit dem gewählten Dateinamen
    file_path <- file.path(zendesk_dir, filename)
    saveRDS(all_tickets_df, file = file_path)
    message("Daten erfolgreich gespeichert unter: ", file_path)
  }

  return(all_tickets_df)
}
