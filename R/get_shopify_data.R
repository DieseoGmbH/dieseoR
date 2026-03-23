#' @title Fetch Data from Shopify API (with Pagination)
#'
#' @description Ruft Daten von einem spezifizierten Shopify API-Endpunkt ab und paginiert
#' automatisch durch alle verfuegbaren Seiten (Cursor-basierte Paginierung via HTTP Link Header).
#' Nutzt `httr::RETRY`, um Rate-Limiting abzufangen.
#'
#' @param shop_url Character. Die Shopify-Shop-URL (Standard: "pummmys.myshopify.com").
#' @param api_key Character. Das Shopify Admin API Access Token (generiert via OAuth).
#' @param endpoint Character. Der gewuenschte API-Endpunkt (z.B. "orders", "products").
#' @param api_version Character. Die zu verwendende Shopify API-Version (Standard: "2024-01").
#' @param limit Integer. Anzahl der Eintraege pro Seite (Maximal 250 fuer Shopify).
#'
#' @return Ein tibble mit allen geparsten Daten ueber alle Seiten hinweg.
#'
#' @importFrom httr RETRY add_headers content stop_for_status headers
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble bind_rows
#' @importFrom stringr str_c str_detect str_split str_extract
#' @export
#'
#' @examples \dontrun{
#' # Holt ALLE Bestellungen (in 250er-Schritten)
#' shopify_orders <- get_shopify_data(
#'   api_key = my_shopify_token,
#'   endpoint = "orders"
#' )
#' }
get_shopify_data <- function(shop_url = "pummmys.myshopify.com",
                             api_key,
                             endpoint = "orders",
                             api_version = "2024-01",
                             limit = 250) {
  if (missing(api_key) || api_key == "") {
    stop("Fehler: Es muss ein gueltiger api_key uebergeben werden.")
  }

  # Basis-URL fuer die erste Seite konstruieren (inklusive Limit)
  current_url <- stringr::str_c("https://", shop_url, "/admin/api/", api_version, "/", endpoint, ".json?limit=", limit)

  # Shopify-Falle umgehen: Bei Bestellungen wollen wir standardmaessig ALLE (auch abgeschlossene/alte)
  if (endpoint == "orders") {
    current_url <- stringr::str_c(current_url, "&status=any")
    message("Endpunkt 'orders' erkannt: Fuege Parameter '&status=any' hinzu, um historische Daten abzurufen.")
  }

  all_data_list <- list()
  has_next_page <- TRUE
  page_counter <- 1

  message("Starte Datenabruf vom Endpunkt '", endpoint, "'...")

  while (has_next_page) {
    message("Lade Seite ", page_counter, "...")

    # API-Call
    response <- httr::RETRY(
      verb = "GET",
      url = current_url,
      httr::add_headers(`X-Shopify-Access-Token` = api_key),
      times = 3,
      pause_base = 1,
      pause_cap = 60
    )

    # Fehlerhandling
    httr::stop_for_status(response, task = stringr::str_c("Fetch data from Shopify page ", page_counter))

    # JSON Content parsen
    raw_content <- httr::content(response, as = "text", encoding = "UTF-8")
    parsed_data <- jsonlite::fromJSON(raw_content, flatten = TRUE)

    # Als Tibble in unsere Liste packen
    current_tibble <- parsed_data[[endpoint]] |> dplyr::as_tibble()
    all_data_list <- append(all_data_list, list(current_tibble))

    # Paginierung pruefen: Gibt es einen "Link" Header?
    link_header <- httr::headers(response)$link

    if (!is.null(link_header) && stringr::str_detect(link_header, 'rel="next"')) {
      # Shopify packt manchmal "previous" und "next" in den gleichen String, getrennt durch Komma
      links <- stringr::str_split(link_header, ",")[[1]]

      # Nur den Teil mit "next" isolieren
      next_link_raw <- links[stringr::str_detect(links, 'rel="next"')]

      # Die tatsaechliche URL steht zwischen spitzen Klammern < >
      # Wir nutzen stringr mit einer Regex (Lookbehind) um alles zwischen < und > zu extrahieren
      current_url <- stringr::str_extract(next_link_raw, "(?<=<)[^>]+")

      page_counter <- page_counter + 1
    } else {
      # Kein "next" Link gefunden -> Wir sind auf der letzten Seite
      has_next_page <- FALSE
    }
  }

  # Alle Seiten zu einem grossen Datensatz zusammenfuehren
  final_dataset <- all_data_list |> dplyr::bind_rows()

  message("Fertig! Erfolgreich ", nrow(final_dataset), " Eintreage in ", page_counter, " Seiten geladen.")

  return(final_dataset)
}
