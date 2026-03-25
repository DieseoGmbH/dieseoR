#' @title Inkrementelles Update der PayPal Daten (Upsert)
#'
#' @description Liest die bestehende PayPal `.rds` Datei ein, ermittelt das
#' aktuellste Transaktionsdatum, zieht neue Daten via API und fuehrt einen
#' deduplizierten Upsert durch.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param data_path Character. Der direkte Dateipfad zur `.rds` Datei.
#' Überschreibt standardmäßig die Konstruktion aus `datadir`.
#'
#' @return Invisible TRUE bei Erfolg.
#' @export
#'
#' @importFrom dplyr bind_rows distinct filter
#'
#' @examples \dontrun{
#' # Standardaufruf (nutzt ~/data/paypal/...)
#' update_paypal_data(datadir = "~/data")
#'
#' # Spezifischer Aufruf aus dem Shiny-Verzeichnis
#' update_paypal_data(data_path = "data/all_paypal_transactions.rds")
#' }
update_paypal_data <- function(datadir = "~/data",
                               data_path = file.path(datadir, "paypal", "all_paypal_transactions.rds")) {
  token <- get_paypal_token()

  # 1. Pruefen, ob die Historie ueberhaupt existiert
  if (!file.exists(data_path)) {
    stop(sprintf("Keine Master-RDS unter '%s' gefunden. Bitte fuehre zuerst den Initial Load aus.", data_path), call. = FALSE)
  }

  message(sprintf("Lese bestehende Datenbank ein aus: %s", data_path))
  df_existing <- readRDS(data_path)

  # 2. Startdatum fuer den API-Call ermitteln
  # PayPal speichert das Datum meist in 'transaction_info.transaction_initiation_date'
  # Format ist ISO8601 (z.B. "2026-03-23T12:00:00Z")
  if ("transaction_info.transaction_initiation_date" %in% names(df_existing)) {
    # Wir schneiden den Zeitstempel ab, um nur das Datum (YYYY-MM-DD) zu bekommen
    dates <- as.Date(substr(df_existing$transaction_info.transaction_initiation_date, 1, 10))
    max_date <- max(dates, na.rm = TRUE)

    # DevOps Best Practice: Ueberlappung!
    # Wir gehen immer 2 Tage in die Vergangenheit zurueck. So fangen wir
    # Transaktionen auf, die PayPal gestern wg. Indexierungs-Delays verschluckt hat.
    start_fetch <- max_date - 2
  } else {
    # Fallback, falls das Format komisch ist
    message("Konnte kein Datumsfeld finden. Lade die letzten 7 Tage als Fallback.")
    start_fetch <- Sys.Date() - 7
  }

  end_fetch <- Sys.Date()

  message(sprintf("Starte Inkrementelles Update von %s bis %s...", start_fetch, end_fetch))

  # 3. Neue Daten ziehen
  df_new <- get_paypal_transactions(
    token = token,
    start_date = start_fetch,
    end_date = end_fetch
  )

  if (nrow(df_new) == 0) {
    message("Keine neuen Transaktionen gefunden. Datenbank ist aktuell.")
    return(invisible(TRUE))
  }

  # 4. Daten anhaengen und deduplizieren (Der eigentliche Upsert)
  df_combined <- dplyr::bind_rows(df_existing, df_new)

  if ("transaction_info.transaction_id" %in% names(df_combined)) {
    # Wir behalten bei Duplikaten die aktuellste Zeile (aus df_new), da sich
    # der Status (z.B. "REFUNDED") geaendert haben koennte
    df_combined <- df_combined |>
      dplyr::distinct(transaction_info.transaction_id, .keep_all = TRUE)
  } else {
    # Fallback: Kompletten Row-Match deduplizieren
    df_combined <- dplyr::distinct(df_combined)
  }

  # 5. Datenbank ueberschreiben
  saveRDS(df_combined, data_path)
  message(sprintf(
    "Upsert erfolgreich! %s neue/geupdatete Datensaetze integriert. Gesamt: %s",
    nrow(df_new), nrow(df_combined)
  ))

  return(invisible(TRUE))
}
