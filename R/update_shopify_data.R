#' @title Inkrementelles Update der Shopify Daten (Upsert)
#'
#' @description Zieht geaenderte Daten via API, entpackt diese dynamisch je nach
#' Endpunkt und fuehrt einen deduplizierten Upsert durch.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param endpoint Character. Welcher Endpunkt aktualisiert werden soll ("orders", "checkouts", "products", "customers").
#' @param api_key Character. Das Shopify Admin API Access Token.
#' @param data_path Optional. Manueller Pfad zur RDS-Datei. Wird sonst automatisch berechnet.
#'
#' @return Invisible TRUE bei Erfolg.
#' @export
#'
#' @importFrom dplyr bind_rows distinct filter
#' @importFrom lubridate days
update_shopify_data <- function(datadir = "~/data",
                                endpoint = "orders",
                                api_key,
                                data_path = NULL) {
  if (missing(api_key) || api_key == "") {
    stop("Fehler: api_key muss uebergeben werden.", call. = FALSE)
  }

  # 1. Automatische Pfad-Generierung falls nicht manuell uebergeben
  if (is.null(data_path)) {
    if (endpoint == "orders") {
      data_path <- file.path(datadir, "shopify", "all_shopify_items.rds")
    } else {
      data_path <- file.path(datadir, "shopify", sprintf("all_shopify_%s.rds", endpoint))
    }
  }

  # 2. Pruefen, ob die Historie ueberhaupt existiert
  if (!file.exists(data_path)) {
    stop(sprintf("Keine Master-RDS unter '%s' gefunden. Bitte fuehre zuerst den Initial Load aus.", data_path), call. = FALSE)
  }

  message(sprintf("Lese bestehende '%s' Datenbank ein aus: %s", endpoint, data_path))
  df_existing <- readRDS(data_path)

  # 3. Startdatum fuer den API-Call ermitteln (Korrektur: Nutzt updated_at, nicht created_at)
  if ("updated_at" %in% names(df_existing)) {
    max_date <- as.Date(max(df_existing$updated_at, na.rm = TRUE))
    start_fetch <- max_date - lubridate::days(2)
  } else {
    message("Konnte kein 'updated_at' finden. Lade die letzten 7 Tage als Fallback.")
    start_fetch <- Sys.Date() - lubridate::days(7)
  }

  updated_at_min <- format(start_fetch, "%Y-%m-%dT00:00:00Z")
  message(sprintf("Starte inkrementelles Update fuer '%s' ab %s...", endpoint, updated_at_min))

  # 4. Neue Daten von der API ziehen
  df_new_raw <- get_shopify_data(
    api_key = api_key,
    endpoint = endpoint,
    updated_at_min = updated_at_min
  )

  if (nrow(df_new_raw) == 0) {
    message("Keine neuen oder geupdateten Daten gefunden. Datenbank ist aktuell.")
    return(invisible(TRUE))
  }

  message(sprintf("%s veränderte Datensaetze geladen. Starte Bereinigung...", nrow(df_new_raw)))

  # 5. Daten bereinigen (Uebergibt den Endpunkt!)
  df_new_clean <- clean_up_shopify(df_new_raw, endpoint = endpoint)
  rm(df_new_raw)
  gc()

  # 6. Daten anhaengen (Neue Daten GANZ OBEN fuer die Deduplizierung)
  df_combined <- dplyr::bind_rows(df_new_clean, df_existing)

  # 7. Dynamische Deduplizierung je nach Endpunkt
  df_combined <- switch(endpoint,
    "orders" = dplyr::distinct(df_combined, item_id, .keep_all = TRUE),
    "products" = dplyr::distinct(df_combined, variants_id, .keep_all = TRUE),
    "customers" = dplyr::distinct(df_combined, id, .keep_all = TRUE),
    # Bei Checkouts brauchen wir Order_id & item_id, da ein Checkout mehrere Items hat
    "checkouts" = dplyr::distinct(df_combined, order_id, item_id, .keep_all = TRUE),
    dplyr::distinct(df_combined) # Fallback
  )

  # 8. Datenbank ueberschreiben
  saveRDS(df_combined, data_path)
  message(sprintf(
    "Upsert erfolgreich! %s neue/aktualisierte '%s' integriert. Gesamtbestand: %s.",
    nrow(df_new_clean), endpoint, nrow(df_combined)
  ))

  return(invisible(TRUE))
}
