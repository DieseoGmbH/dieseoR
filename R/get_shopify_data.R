#' @title Fetch Data from Shopify API (with Pagination & Checkpointing)
#'
#' @description Ruft Daten von einem spezifizierten Shopify API-Endpunkt ab und paginiert
#' automatisch durch alle verfuegbaren Seiten. Integriert Feld-Filterung zur Performance-Optimierung
#' und speichert Zwischenstaende, um Datenverlust bei abgelaufenen Tokens (HTTP 401) zu vermeiden.
#'
#' @param shop_url Character. Die Shopify-Shop-URL (Standard: "pummmys.myshopify.com").
#' @param api_key Character. Das Shopify Admin API Access Token (generiert via OAuth).
#' @param endpoint Character. Der gewuenschte API-Endpunkt (z.B. "orders", "products").
#' @param api_version Character. Die zu verwendende Shopify API-Version (Standard: "2024-01").
#' @param limit Integer. Anzahl der Eintraege pro Seite (Maximal 250 fuer Shopify).
#' @param fields Character. Optional. Kommaseparierte Liste der gewuenschten Spalten (z.B. "id,created_at,total_price").
#' @param checkpoint_dir Character. Optional. Lokaler Pfad zur Zwischenspeicherung (z.B. "~/data/").
#'
#' @return Ein tibble mit allen geparsten Daten. Bei einem HTTP 401 Abbruch werden die bis dahin geladenen Daten zurueckgegeben.
#'
#' @importFrom httr RETRY add_headers content stop_for_status headers status_code
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble bind_rows
#' @importFrom stringr str_c str_detect str_split str_extract
#' @export
get_shopify_data <- function(shop_url = "pummmys.myshopify.com",
                             api_key,
                             endpoint = "orders",
                             api_version = "2024-01",
                             limit = 250,
                             fields = NULL,
                             checkpoint_dir = NULL) {
  if (missing(api_key) || api_key == "") {
    stop("Fehler: Es muss ein gueltiger api_key uebergeben werden.")
  }

  # Basis-URL konstruieren
  current_url <- stringr::str_c("https://", shop_url, "/admin/api/", api_version, "/", endpoint, ".json?limit=", limit)

  # Spalten-Filterung (erhoeht Performance enorm)
  if (!is.null(fields)) {
    current_url <- stringr::str_c(current_url, "&fields=", fields)
    message("Spaltenfilter aktiv: ", fields)
  }

  if (endpoint == "orders") {
    current_url <- stringr::str_c(current_url, "&status=any")
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

    # Graceful Exit bei Token-Ablauf (401)
    if (httr::status_code(response) == 401) {
      warning("HTTP 401 Unauthorized auf Seite ", page_counter, ". Token ist vermutlich abgelaufen. Schleife wird abgebrochen, bisherige Daten werden gerettet!")
      break
    }

    # Normales Fehlerhandling fuer andere HTTP-Fehler
    httr::stop_for_status(response, task = stringr::str_c("Fetch data from Shopify page ", page_counter))

    # JSON Content parsen
    raw_content <- httr::content(response, as = "text", encoding = "UTF-8")
    parsed_data <- jsonlite::fromJSON(raw_content, flatten = TRUE)

    current_tibble <- parsed_data[[endpoint]] |> dplyr::as_tibble()
    all_data_list <- append(all_data_list, list(current_tibble))

    # Optionales Checkpointing alle 500 Seiten
    if (!is.null(checkpoint_dir) && (page_counter %% 500 == 0)) {
      checkpoint_path <- file.path(checkpoint_dir, stringr::str_c("shopify_", endpoint, "_checkpoint_", page_counter, ".rds"))
      temp_dataset <- all_data_list |> dplyr::bind_rows()
      saveRDS(temp_dataset, checkpoint_path)
      message("Checkpoint gespeichert unter: ", checkpoint_path)
    }

    # Paginierung pruefen
    link_header <- httr::headers(response)$link

    if (!is.null(link_header) && stringr::str_detect(link_header, 'rel="next"')) {
      links <- stringr::str_split(link_header, ",")[[1]]
      next_link_raw <- links[stringr::str_detect(links, 'rel="next"')]
      current_url <- stringr::str_extract(next_link_raw, "(?<=<)[^>]+")
      page_counter <- page_counter + 1
    } else {
      has_next_page <- FALSE
    }
  }

  # Finale Daten zusammenfuehren
  if (length(all_data_list) == 0) {
    message("Keine Daten geladen.")
    return(dplyr::tibble())
  }

  final_dataset <- all_data_list |> dplyr::bind_rows()
  message("Fertig! Erfolgreich ", nrow(final_dataset), " Eintraege aus ", page_counter - 1, " Seiten extrahiert.")

  return(final_dataset)
}
