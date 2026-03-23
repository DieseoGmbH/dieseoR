#' @title Fetch Trustpilot Reviews
#' @description Ruft paginierte Kundenbewertungen von der Trustpilot API ab.
#' Integriert httr::RETRY für robustes Rate-Limiting.
#'
#' @param business_unit_id Character. Die Trustpilot Business Unit ID (Standard via .Renviron).
#' @param api_key Character. Der Trustpilot API Key (Standard via .Renviron).
#' @param max_pages Numeric. Maximale Anzahl an Seiten, die abgerufen werden sollen (Sicherheitsnetz).
#'
#' @return Ein aufbereitetes `tibble` mit den abgerufenen Bewertungen.
#' @export
#'
#' @importFrom httr RETRY add_headers content status_code
#' @importFrom dplyr bind_rows mutate select as_tibble
#' @importFrom purrr map
#' @importFrom stringr str_c
#'
#' @examples \dontrun{
#' # Lädt Bewertungen mit Credentials aus der .Renviron
#' tp_data <- get_trustpilot_data()
#' }
get_trustpilot_data <- function(business_unit_id = Sys.getenv("TRUSTPILOT_BUSINESS_UNIT"),
                                api_key = Sys.getenv("TRUSTPILOT_API_KEY"),
                                max_pages = 100) {
  # 1. Robustes Error-Handling: Inputs prüfen
  if (business_unit_id == "" || api_key == "") {
    stop("API Key oder Business Unit ID fehlen. Bitte .Renviron prüfen.", call. = FALSE)
  }

  base_url <- stringr::str_c(
    "https://api.trustpilot.com/v1/business-units/",
    business_unit_id,
    "/reviews"
  )

  all_reviews <- list()
  has_next_page <- TRUE
  current_page <- 1

  message("Starte Trustpilot API-Abruf...")

  # 2. Extract: Pagination & Rate-Limiting (httr::RETRY)
  while (has_next_page && current_page <= max_pages) {
    # Nutze RETRY um 429 (Too Many Requests) oder 500er Fehler abzufangen
    res <- httr::RETRY(
      verb = "GET",
      url = base_url,
      query = list(page = current_page, perPage = 100),
      httr::add_headers(apikey = api_key),
      times = 3, # Maximal 3 Versuche
      pause_base = 2, # Exponentielles Backoff
      pause_cap = 10
    )

    # API Fehler abfangen
    if (httr::status_code(res) != 200) {
      stop(
        stringr::str_c("Trustpilot API Error auf Seite ", current_page, " - Status: ", httr::status_code(res)),
        call. = FALSE
      )
    }

    # Content parsen
    parsed_data <- httr::content(res, as = "parsed", type = "application/json")

    # Wenn keine Reviews auf der Seite sind, abbrechen
    if (length(parsed_data$reviews) == 0) {
      has_next_page <- FALSE
      break
    }

    # Extrahierte Daten in Liste speichern
    all_reviews[[current_page]] <- parsed_data$reviews |>
      purrr::map(~ as.data.frame(t(unlist(.x)), stringsAsFactors = FALSE)) |>
      dplyr::bind_rows() |>
      dplyr::as_tibble()

    # Pagination-Check
    if (is.null(parsed_data$links) || !any(sapply(parsed_data$links, function(x) x$rel == "next-page"))) {
      has_next_page <- FALSE
    } else {
      current_page <- current_page + 1
    }
  }

  # 3. Transform: Zusammenführen und erste Bereinigung
  final_df <- dplyr::bind_rows(all_reviews)

  message(stringr::str_c("Erfolgreich ", nrow(final_df), " Trustpilot-Bewertungen geladen."))

  return(final_df)
}
