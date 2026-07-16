#' @title Fetch All Orders from Dashboardly API
#' @description Holt alle paginierten Bestelldaten für einen spezifischen Shop
#'   über die Dashboardly Developer API. Iteriert automatisch durch alle Seiten.
#' @param shop_id character. Die ID des Shops (z. B. "3d2ec700-...").
#' @param start_date character. Startdatum im Format "YYYY-MM-DD".
#' @param end_date character. Enddatum im Format "YYYY-MM-DD".
#' @param api_key character. Dashboardly API Key. Standardmäßig via `.Renviron`.
#' @return Ein Tibble mit allen Bestellungen des angegebenen Zeitraums.
#' @importFrom httr RETRY add_headers status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble bind_rows
#' @export
#' @examples \dontrun{
#' tiktok_orders <- get_dashboardly_orders(
#'   shop_id = "3d2ec700-f2a3-468b-91ba-ec8826b6ab43",
#'   start_date = "2026-07-01",
#'   end_date = "2026-07-08"
#' )
#' }
get_dashboardly_data <- function(shop_id,
                                 start_date,
                                 end_date,
                                 api_key = Sys.getenv("DASHBOARDLY_API_KEY")) {
  # 1. Validierung der Inputs
  if (api_key == "") {
    stop("API-Key fehlt! Bitte in der .Renviron als DASHBOARDLY_API_KEY setzen.")
  }
  if (missing(shop_id) || missing(start_date) || missing(end_date)) {
    stop("shop_id, start_date und end_date sind Pflichtfelder.")
  }

  base_url <- "https://api.dashboardly.io/api/developer/v1/orders"

  # 2. Setup für Pagination
  all_orders <- list()
  current_offset <- 0
  has_more <- TRUE

  message(sprintf(
    "Starte Dashboardly Datenabruf für Shop %s (Zeitraum: %s bis %s)...",
    shop_id, start_date, end_date
  ))

  # 3. Pagination-Loop (blättert durch alle Seiten)
  while (has_more) {
    # Robuster API-Aufruf
    res <- httr::RETRY(
      verb = "GET",
      url = base_url,
      httr::add_headers(Authorization = paste("Bearer", api_key)),
      query = list(
        shopId = shop_id,
        startDate = start_date,
        endDate = end_date,
        offset = current_offset,
        limit = 50 # Sicherheitshalber explizit mitgeben
      ),
      times = 3,
      pause_base = 2,
      pause_cap = 10
    )

    # Fehlerprüfung
    if (httr::status_code(res) != 200) {
      error_msg <- httr::content(res, as = "text", encoding = "UTF-8")
      stop("Dashboardly API-Fehler! Status: ", httr::status_code(res), " | Message: ", error_msg)
    }

    # Daten Parsen
    parsed_json <- httr::content(res, as = "text", encoding = "UTF-8")
    parsed_data <- jsonlite::fromJSON(parsed_json)

    # Check, ob Daten da sind
    page_orders <- parsed_data$data$orders
    if (is.null(page_orders) || nrow(page_orders) == 0) {
      warning("API lieferte unerwartet leere Daten auf Seite mit Offset ", current_offset)
      break
    }

    # In Liste speichern
    all_orders[[length(all_orders) + 1]] <- page_orders

    # 4. Pagination-Metadaten updaten für den nächsten Durchlauf
    meta_pag <- parsed_data$meta$pagination
    current_offset <- current_offset + meta_pag$limit
    has_more <- meta_pag$hasMore
    total_records <- meta_pag$total

    # Fortschritt ausgeben
    geladen <- min(current_offset, total_records)
    message(sprintf("✅ %d von %d Bestellungen geladen...", geladen, total_records))

    # Best-Practice: API etwas atmen lassen, um Rate-Limits zu vermeiden
    Sys.sleep(0.3)
  }

  # 5. Alle Listen-Elemente zu einem großen Tibble verschmelzen
  final_df <- dplyr::bind_rows(all_orders) |>
    dplyr::as_tibble()

  message("🎉 Abruf erfolgreich abgeschlossen! Gesamt: ", nrow(final_df), " Zeilen.")

  return(final_df)
}
