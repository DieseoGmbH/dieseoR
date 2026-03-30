#' @title Fetch Data from Shopify API (with Pagination and Checkpointing)
#'
#' @description Ruft Daten von einem spezifizierten Shopify API-Endpunkt ab.
#' Nutzt Chunking, um bei grossen Datenmengen (z. B. > 100.000 Bestellungen) den Arbeitsspeicher
#' zu schonen. Speichert Zwischenstaende als .rds-Dateien.
#'
#' @param shop_url Character. Die Shopify-Shop-URL.
#' @param api_key Character. Das Shopify Admin API Access Token.
#' @param endpoint Character. Der gewuenschte API-Endpunkt.
#' @param api_version Character. Die zu verwendende Shopify API-Version.
#' @param limit Integer. Anzahl der Eintraege pro Seite (Max: 250).
#' @param chunk_size Integer. Nach wie vielen Seiten soll ein Zwischenstand auf der Festplatte gespeichert werden?
#' @param temp_dir Character. Pfad zum Verzeichnis, in dem die Chunks gespeichert werden.
#'
#' @return Ein tibble mit allen geparsten Daten.
#'
#' @importFrom httr RETRY add_headers content stop_for_status headers
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble bind_rows
#' @importFrom stringr str_c str_detect str_split str_extract
#' @export
get_shopify_data <- function(shop_url = "pummmys.myshopify.com",
                             api_key,
                             endpoint = "orders",
                             api_version = "2024-01",
                             limit = 250,
                             chunk_size = 100,
                             temp_dir = "~/data/shopify/shopify_temp",
                             updated_at_min = NULL) { # <-- NEUER PARAMETER

  if (missing(api_key) || api_key == "") stop("Fehler: api_key fehlt.")

  # Temporaeres Verzeichnis erstellen, falls es nicht existiert
  if (!dir.exists(temp_dir)) {
    dir.create(temp_dir, recursive = TRUE)
  }

  current_url <- stringr::str_c("https://", shop_url, "/admin/api/", api_version, "/", endpoint, ".json?limit=", limit)

  if (endpoint == "orders") {
    current_url <- stringr::str_c(current_url, "&status=any")
  }

  # NEUE LOGIK: Wenn ein Datum uebergeben wurde, hange es an die URL an
  if (!is.null(updated_at_min)) {
    current_url <- stringr::str_c(current_url, "&updated_at_min=", updated_at_min)
    message("Inkrementeller Load aktiv: Filtere ab ", updated_at_min)
  }

  # ... ab hier geht dein bestehender Code mit der while(has_next_page) Schleife weiter ...
  all_data_list <- list()
  has_next_page <- TRUE
  page_counter <- 1
  chunk_counter <- 1

  message("Starte Datenabruf. Speichere Zwischenstand alle ", chunk_size, " Seiten.")

  while (has_next_page) {
    message("Lade Seite ", page_counter, "...")

    response <- httr::RETRY(
      verb = "GET", url = current_url, httr::add_headers(`X-Shopify-Access-Token` = api_key),
      times = 5, pause_base = 2, pause_cap = 60 # Retry-Logik etwas robuster gemacht
    )

    httr::stop_for_status(response, task = stringr::str_c("Fetch page ", page_counter))

    raw_content <- httr::content(response, as = "text", encoding = "UTF-8")
    parsed_data <- jsonlite::fromJSON(raw_content, flatten = TRUE)

    current_tibble <- parsed_data[[endpoint]] |> dplyr::as_tibble()
    all_data_list <- append(all_data_list, list(current_tibble))

    # CHECKPOINT-LOGIK: Arbeitsspeicher leeren und Daten sichern
    if (page_counter %% chunk_size == 0) {
      chunk_file <- file.path(temp_dir, stringr::str_c("shopify_", endpoint, "_chunk_", chunk_counter, ".rds"))
      message("\n---> Speichere Chunk ", chunk_counter, " in ", chunk_file)

      chunk_data <- all_data_list |> dplyr::bind_rows()
      saveRDS(chunk_data, file = chunk_file)

      # RAM massiv entlasten!
      all_data_list <- list()
      gc() # Garbage Collector zwingen, den Speicher freizugeben

      chunk_counter <- chunk_counter + 1
      Sys.sleep(1) # Kurze Atempause fuer die Shopify API
    }

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

  # Den verbleibenden Rest (der nicht genau durch chunk_size teilbar war) speichern
  if (length(all_data_list) > 0) {
    chunk_file <- file.path(temp_dir, stringr::str_c("shopify_", endpoint, "_chunk_", chunk_counter, ".rds"))
    message("\n---> Speichere finalen Chunk ", chunk_counter, " in ", chunk_file)
    chunk_data <- all_data_list |> dplyr::bind_rows()
    saveRDS(chunk_data, file = chunk_file)
  }

  # Alle Chunks von der Festplatte laden und zu einem finalen Tibble mergen
  message("Lade alle gesicherten Chunks zusammen...")
  all_files <- list.files(temp_dir, pattern = stringr::str_c("shopify_", endpoint, "_chunk_"), full.names = TRUE)

  final_dataset <- lapply(all_files, readRDS) |>
    dplyr::bind_rows()

  message("Fertig! Erfolgreich ", nrow(final_dataset), " Eintreage geladen.")

  # Optional: Temp-Files aufraeumen (auskommentiert zur Sicherheit)
  # unlink(all_files)

  return(final_dataset)
}
