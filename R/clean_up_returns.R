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
    # 2b. Soft-Delete des Portals als Flag retten
    #
    # ⚠️ NEU 26.08.2026. `deleted_at` wurde bisher zusammen mit den PII-Spalten
    # verworfen. Das war folgenschwer: das Retourenportal *löscht* abgearbeitete
    # bzw. verfallene Vorgänge weich, statt ihren Status zu ändern. 233.050 von
    # 472.023 Rohzeilen (49,4 %) tragen einen `deleted_at`-Stempel und stehen
    # trotzdem weiter auf "requested" oder "approved".
    #
    # Ohne dieses Flag zählt jede Rückstau-Auswertung sie als offen. Der Effekt
    # ist nicht klein und nicht gleichverteilt — er trifft fast ausschliesslich
    # die alten Vorgänge:
    #     Alter der offenen Vorgänge   Anteil gelöscht
    #     0–7 Tage                       1,2 %
    #     31–90 Tage                     4,0 %
    #     91–365 Tage                   20,6 %
    #     > 365 Tage                    99,95 %
    # Der vermeintliche Ein-Jahres-Rückstau von 45.671 Vorgängen schrumpft mit
    # dem Flag auf 16. Allein am 20.06.2025 hat das Portal 101.169 Vorgänge auf
    # einen Schlag weggeräumt (derselbe Tag, an dem auch 39.110 Vorgänge in
    # einem Rutsch geschlossen wurden).
    #
    # Der Zeitstempel selbst bleibt draussen (er hilft fachlich nicht), das
    # Flag genügt und kostet ein Byte pro Zeile.
    dplyr::mutate(ist_geloescht = !is.na(deleted_at) & deleted_at != "") |>
    # 3. Unnötige und sensible Spalten (DSGVO) rauswerfen
    dplyr::select(
      -c(
        shopify_order_path, full_name, phone, shopify_new_order_path,
        deleted_at, delivered_at, requested_wrong_items,
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
