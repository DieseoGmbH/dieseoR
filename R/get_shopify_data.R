#' @title Fetch Data from Shopify API (with Pagination and Checkpointing)
#'
#' @description Ruft Daten von einem spezifizierten Shopify API-Endpunkt ab.
#' Nutzt Chunking, um bei grossen Datenmengen (z. B. > 100.000 Bestellungen) den Arbeitsspeicher
#' zu schonen. Speichert Zwischenstaende als .rds-Dateien ab und nutzt eine Run-ID, um Ueberschreibungen zu verhindern.
#'
#' @param shop_url Character. Die Shopify-Shop-URL.
#' @param api_key Character. Das Shopify Admin API Access Token.
#' @param endpoint Character. Der gewuenschte API-Endpunkt.
#' @param api_version Character. Die zu verwendende Shopify API-Version.
#' @param limit Integer. Anzahl der Eintraege pro Seite (Max: 250).
#' @param chunk_size Integer. Nach wie vielen Seiten soll ein Zwischenstand auf der Festplatte gespeichert werden?
#' @param raw_dir Character. Pfad zum Verzeichnis, in dem die Chunks permanent gespeichert werden (Data Lake).
#' @param updated_at_min Character. Optionales Startdatum (ISO 8601) fuer inkrementelle Updates basierend auf Aenderungen.
#' @param created_at_min Character. Optionales Startdatum (ISO 8601) fuer das Erstellungsdatum.
#' @param created_at_max Character. Optionales Enddatum (ISO 8601) fuer das Erstellungsdatum (nuetzlich fuer Resume-Loads in die Vergangenheit).
#'
#' @return Ein tibble mit allen geparsten Daten des aktuellen Runs.
#'
#' @importFrom httr RETRY add_headers timeout content stop_for_status headers
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
                             raw_dir = "~/data/shopify/raw_chunks",
                             updated_at_min = NULL,
                             created_at_min = NULL,
                             created_at_max = NULL) {
  if (missing(api_key) || api_key == "") stop("Fehler: api_key fehlt.")

  # Ordner erstellen, falls er nicht existiert
  if (!dir.exists(raw_dir)) {
    dir.create(raw_dir, recursive = TRUE)
  }

  # NEU: Einzigartige Run-ID (Timestamp) für diesen Abruf generieren
  run_id <- format(Sys.time(), "%Y%m%d_%H%M%S")

  current_url <- stringr::str_c("https://", shop_url, "/admin/api/", api_version, "/", endpoint, ".json?limit=", limit)

  if (endpoint == "orders") {
    current_url <- stringr::str_c(current_url, "&status=any")
  }

  # --- NEU: Dynamische URL-Filter anhängen ---
  if (!is.null(updated_at_min)) {
    current_url <- stringr::str_c(current_url, "&updated_at_min=", updated_at_min)
    message("Inkrementeller Load aktiv: Filtere auf Update ab ", updated_at_min)
  }

  if (!is.null(created_at_min)) {
    current_url <- stringr::str_c(current_url, "&created_at_min=", created_at_min)
    message("Filter aktiv: Lade nur Daten ERSTELLT AB ", created_at_min)
  }

  if (!is.null(created_at_max)) {
    current_url <- stringr::str_c(current_url, "&created_at_max=", created_at_max)
    message("Resume aktiv: Lade nur Daten ERSTELLT VOR ", created_at_max)
  }

  all_data_list <- list()
  has_next_page <- TRUE
  page_counter <- 1
  chunk_counter <- 1

  message("Starte Datenabruf (Run ID: ", run_id, "). Speichere Chunks in ", raw_dir)

  while (has_next_page) {
    message("Lade Seite ", page_counter, "...")

    # NEU: httr::timeout(60) hinzugefügt, damit das Skript bei Verbindungsabbrüchen nicht einfriert!
    response <- httr::RETRY(
      verb = "GET", url = current_url,
      httr::add_headers(`X-Shopify-Access-Token` = api_key),
      httr::timeout(60),
      times = 5, pause_base = 2, pause_cap = 60
    )

    httr::stop_for_status(response, task = stringr::str_c("Fetch page ", page_counter))

    raw_content <- httr::content(response, as = "text", encoding = "UTF-8")
    parsed_data <- jsonlite::fromJSON(raw_content, flatten = TRUE)

    current_tibble <- parsed_data[[endpoint]] |> dplyr::as_tibble()
    all_data_list <- append(all_data_list, list(current_tibble))

    if (page_counter %% chunk_size == 0) {
      # NEU: Dateiname mit Timestamp! Kein Überschreiben der historischen Chunks mehr.
      chunk_file <- file.path(raw_dir, sprintf("shopify_%s_%s_chunk_%03d.rds", endpoint, run_id, chunk_counter))
      message("\n---> Speichere Chunk ", chunk_counter, " in ", chunk_file)

      chunk_data <- all_data_list |> dplyr::bind_rows()
      saveRDS(chunk_data, file = chunk_file)

      all_data_list <- list()
      gc()

      chunk_counter <- chunk_counter + 1
      Sys.sleep(1)
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

  if (length(all_data_list) > 0) {
    chunk_file <- file.path(raw_dir, sprintf("shopify_%s_%s_chunk_%03d.rds", endpoint, run_id, chunk_counter))
    message("\n---> Speichere finalen Chunk ", chunk_counter, " in ", chunk_file)
    chunk_data <- all_data_list |> dplyr::bind_rows()
    saveRDS(chunk_data, file = chunk_file)
  }

  message("Lade alle Chunks DIESES RUNS zusammen...")
  # Er lädt jetzt nur die Chunks zusammen, die exakt diesen Timestamp im Namen haben!
  all_files <- list.files(raw_dir, pattern = sprintf("shopify_%s_%s_chunk_", endpoint, run_id), full.names = TRUE)

  final_dataset <- lapply(all_files, readRDS) |>
    dplyr::bind_rows()

  message("Fertig! Erfolgreich ", nrow(final_dataset), " Einträge geladen.")
  return(final_dataset)
}
