#' @title Bereinige und formatiere die rohen PayPal-Daten
#'
#' @description Wendet \code{clean_master()} an, reduziert die Spalten drastisch
#' (Whitelist-Ansatz), benennt sie lesbar um und standardisiert die
#' internationalen Ratenzahlungs-Bezeichnungen.
#'
#' @param df Ein Data Frame mit den rohen PayPal-Daten.
#'
#' @return Ein bereinigter Data Frame (Tibble), optimiert für das Shiny Dashboard.
#' @export
#'
#' @importFrom dplyr select mutate case_when coalesce
#' @importFrom stringr str_detect
#' @importFrom lubridate ymd_hms
#'
#' @examples
#' \dontrun{
#' clean_paypal <- clean_up_paypal(raw_paypal_df)
#' }
clean_up_paypal <- function(df) {
  if (!is.data.frame(df)) {
    stop("Der Input muss ein Dataframe sein.")
  }

  cleaned_df <- df |>
    clean_master() |> # Nutzt deinen universellen Vorwäscher

    # 1. Whitelist: Nur die essenziellen Spalten behalten & direkt umbenennen!
    dplyr::select(
      transaction_id = transaction_info_transaction_id,
      date_updated = transaction_info_transaction_updated_date,
      status = transaction_info_transaction_status,
      instrument_type = transaction_info_instrument_type,
      instrument_sub_type = transaction_info_instrument_sub_type,
      currency = transaction_info_transaction_amount_currency_code,
      amount = transaction_info_transaction_amount_value,
      fee = transaction_info_fee_amount_value,

      # Geodaten für deine Maps
      country_code = payer_info_country_code,
      postal_code = payer_info_address_postal_code,
      phone_country = payer_info_phone_number_country_code
    ) |>
    # 2. Datentypen korrigieren und Werte standardisieren
    dplyr::mutate(
      # Datum in UTC parsen
      date_updated = lubridate::ymd_hms(date_updated, tz = "UTC"),

      # Beträge in echte numerische Werte umwandeln
      amount = as.numeric(amount),
      fee = as.numeric(fee),

      # Fallback für die Ländercodes bauen (Wenn country_code leer ist, nimm die Vorwahl)
      geo_country = dplyr::coalesce(country_code, phone_country),

      # Sub-Type der Zahlungsmethode standardisieren
      instrument_sub_type = dplyr::case_when(
        stringr::str_detect(instrument_sub_type, "wallet") ~ "paypal wallet",
        stringr::str_detect(instrument_sub_type, "30 tagen") ~ "rechnung (30 tage)",
        stringr::str_detect(instrument_sub_type, "3 plazos|3 rate|pay in 3") ~ "ratenzahlung (3 raten)",
        stringr::str_detect(instrument_sub_type, "4x|pay in 4|4 months") ~ "ratenzahlung (4 raten)",
        stringr::str_detect(instrument_sub_type, "ratenzahlung") ~ "ratenzahlung (allgemein)",
        TRUE ~ instrument_sub_type
      )
    ) |>
    # 3. Wir werfen die alten Hilfs-Länderspalten raus, da wir nun 'geo_country' haben
    dplyr::select(-country_code, -phone_country)

  return(cleaned_df)
}
