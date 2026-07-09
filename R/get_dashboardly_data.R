#' @title Fetch Data from Dashboardly Developer API
#' @description Extrahierte schreibgeschützte Metriken (z. B. TikTok Shop Daten)
#'   über die Dashboardly REST API. Integriert httr::RETRY für robustes Rate-Limiting.
#' @param endpoint character. Der spezifische API-Endpunkt (z. B. "sales", "orders").
#' @param api_key character. Dashboardly API Key. Standardmäßig aus der .Renviron geladen.
#' @return Ein Tibble mit den angefragten Daten.
#' @importFrom httr RETRY add_headers status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble
#' @export
#' @examples \dontrun{
#' tiktok_sales <- get_dashboardly_data(endpoint = "sales/tiktok")
#' }
get_dashboardly_data <- function(endpoint,
                                 api_key = Sys.getenv("DASHBOARDLY_API_KEY")) {
  # 1. Validierung
  if (api_key == "") {
    stop("Dashboardly API-Key fehlt! Bitte DASHBOARDLY_API_KEY in der .Renviron setzen.")
  }

  if (missing(endpoint)) {
    stop("Bitte einen API-Endpunkt angeben (z. B. 'sales').")
  }

  # 2. URL & Header Konstruktion
  base_url <- "https://api.dashboardly.io/api/developer/v1"
  req_url <- paste0(base_url, "/", endpoint)

  message("Sende GET-Request an Dashboardly API: ", req_url)

  # 3. Robuster API-Aufruf mit Retry-Logik
  response <- httr::RETRY(
    verb = "GET",
    url = req_url,
    httr::add_headers(Authorization = paste("Bearer", api_key)),
    times = 3, # Maximal 3 Versuche
    pause_base = 2, # Wartezeit startet bei 2 Sekunden
    pause_cap = 10 # Maximal 10 Sekunden zwischen Versuchen
  )

  # 4. Error-Handling
  if (httr::status_code(response) != 200) {
    error_msg <- httr::content(response, as = "text", encoding = "UTF-8")
    stop(
      "Dashboardly API-Fehler. HTTP Status: ", httr::status_code(response),
      " | API Response: ", error_msg
    )
  }

  # 5. Parsing & Transformation
  raw_json <- httr::content(response, as = "text", encoding = "UTF-8")
  parsed_list <- jsonlite::fromJSON(raw_json, flatten = TRUE)

  # Konvertierung in Tibble (Prüfung, ob Daten in einem "data" Array gekapselt sind)
  if ("data" %in% names(parsed_list)) {
    df <- dplyr::as_tibble(parsed_list$data)
  } else {
    df <- dplyr::as_tibble(parsed_list)
  }

  return(df)
}
