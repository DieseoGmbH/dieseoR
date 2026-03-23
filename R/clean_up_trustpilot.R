#' @title Clean Trustpilot Data
#' @description Bereinigt die rohen Trustpilot-API-Daten durch Entfernen unnötiger
#' Link- und Metadaten-Spalten sowie durch die Formatierung von Datums- und Zahlenwerten.
#' Nutzt im ersten Schritt die interne Funktion `clean_master()`, um die Spaltennamen
#' zu standardisieren.
#'
#' @param df Data.frame oder Tibble. Die rohen Trustpilot-Daten aus dem API-Call.
#'
#' @return Ein bereinigtes `tibble` mit den relevanten KPIs.
#' @export
#'
#' @importFrom dplyr select mutate starts_with ends_with contains
#' @importFrom lubridate ymd_hms
#'
#' @examples \dontrun{
#' tp_clean <- clean_up_trustpilot(trustpilot_raw)
#' }
clean_up_trustpilot <- function(df) {
  # 1. Robustes Error-Handling
  if (missing(df) || nrow(df) == 0) {
    stop("Das Input-Dataframe ist leer oder fehlt. Bereinigung abgebrochen.", call. = FALSE)
  }

  # 2. Transform-Pipeline
  cleaned_df <- df |>
    dieseoR::clean_master() |>
    dplyr::select(
      -dplyr::starts_with("links_href_"),
      -dplyr::starts_with("links_method_"),
      -dplyr::starts_with("links_rel_"),
      -dplyr::starts_with("business_unit_links_"),
      -dplyr::starts_with("business_unit_history_"),
      -dplyr::starts_with("consumer_links_href_"),
      -dplyr::starts_with("consumer_links_method_"),
      -dplyr::starts_with("consumer_links_rel_"),
      -dplyr::starts_with("compliance_labels"),
      -dplyr::starts_with("invitation_business_unit_id"),
      -dplyr::starts_with("consumer_display_location"),
      -dplyr::starts_with("company_reply_"),
      -dplyr::starts_with("business_unit"),
      -status,
      -counts_towards_trust_score,
      # Remove this if you want to keep 'stars' (since you convert it later)
    ) |>
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "UTC"),
      experienced_at = lubridate::ymd_hms(experienced_at, tz = "UTC"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "UTC"),
      number_of_likes = as.numeric(number_of_likes),
      consumer_number_of_reviews = as.numeric(consumer_number_of_reviews),
      stars = as.numeric(stars)
    )

  # 3. Informatives Feedback
  message("Trustpilot-Daten erfolgreich bereinigt. Verbleibende Spalten: ", ncol(cleaned_df))

  return(cleaned_df)
}
