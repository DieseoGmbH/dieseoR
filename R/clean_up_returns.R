#' Bereinige und formatiere die rohen Retouren-Daten
#'
#' Diese Funktion nimmt den rohen API-Output der Retouren, wendet
#' `clean_master()` an, konvertiert Datums- und Zahlenformate,
#' entfernt sensible/unnötige Spalten und extrahiert das Land aus der Adresse.
#'
#' @param df Ein Data Frame mit den rohen Retouren-Daten.
#'
#' @return Ein bereinigter Data Frame (Tibble), bereit für die Analyse.
#' @export
#'
#' @importFrom dplyr mutate select filter
#' @importFrom lubridate ymd_hms
#' @importFrom stringr str_replace_all str_to_lower str_trim word
#'
#' @examples
#' \dontrun{
#' clean_returns <- clean_up_returns(raw_returns_df)
#' }
clean_up_returns <- function(df) {
  # Sicherheits-Check: Ist der Input überhaupt ein Dataframe?
  if (!is.data.frame(df)) {
    stop("Der Input muss ein Dataframe sein.")
  }

  cleaned_df <- df |>
    # 1. Allgemeine Basis-Bereinigung (deine eigene Funktion)
    clean_master() |>
    # 2. Zeitstempel und numerische Werte richtig formatieren
    dplyr::mutate(
      created_at            = lubridate::ymd_hms(created_at, tz = "UTC"),
      order_completed_date  = lubridate::ymd_hms(order_completed_date, tz = "UTC"),
      updated_at            = lubridate::ymd_hms(updated_at, tz = "UTC"),
      shopify_created_at    = lubridate::ymd_hms(shopify_created_at, tz = "UTC"),
      fulfilled_at          = lubridate::ymd_hms(fulfilled_at, tz = "UTC"),
      order_amount          = as.numeric(order_amount),
      shipping_cost_applied = as.numeric(shipping_cost_applied),
      shipping_cost         = as.numeric(shipping_cost)
    ) |>
    # 3. Unnötige und sensible Spalten (DSGVO) rauswerfen
    dplyr::select(
      -c(
        shopify_order_path, full_name, phone, shopify_new_order_path,
        deleted_at, delivered_at, requested_wrong_items, shopify_order_id,
        draft_order_id, draft_order_name, tracking_number, barcode_number, email
      )
    ) |>
    # 4. Text-Felder aufräumen und Land extrahieren
    dplyr::mutate(
      payment_method = stringr::str_replace_all(payment_method, ",\\s+", ","),
      # Holt sich das letzte Wort nach dem letzten Komma in der Adresse (meist das Land)
      country = stringr::str_to_lower(stringr::str_trim(stringr::word(address, -1, sep = ",\\s*")))
    )

  cleaned_df
}
