#' @title Inkrementelles Update der Shopify Item-Daten (Upsert)
#'
#' @description Liest die bestehende Shopify `.rds` Datei (Item-Level) ein, ermittelt das
#' aktuellste `updated_at` Datum, zieht neue/geänderte Bestellungen via API, entpackt diese
#' auf Item-Ebene (`clean_up_shopify`) und fuehrt einen deduplizierten Upsert durch.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param endpoint Character. Welcher Endpunkt aktualisiert werden soll (Standard: "orders").
#' @param data_path Character. Der direkte Dateipfad zur `.rds` Datei (Standard: Item-Level Datei).
#' @param api_key Character. Das Shopify Admin API Access Token.
#'
#' @return Invisible TRUE bei Erfolg.
#' @export
#'
#' @importFrom dplyr bind_rows distinct filter
#' @importFrom lubridate days
#' @importFrom stringr str_c
#'
#' @examples \dontrun{
#' update_shopify_data(api_key = Sys.getenv("SHOPIFY_TOKEN"))
#' }
update_shopify_data <- function(datadir = "~/data/shopify",
                                endpoint = "orders",
                                data_path = file.path(datadir, "shopify", "all_shopify_items.rds"),
                                api_key) {
  if (missing(api_key) || api_key == "") {
    stop("Fehler: api_key muss uebergeben werden.", call. = FALSE)
  }

  # 1. Pruefen, ob die Historie ueberhaupt existiert
  if (!file.exists(data_path)) {
    stop(sprintf("Keine Master-RDS unter '%s' gefunden. Bitte fuehre zuerst den Initial Load aus.", data_path), call. = FALSE)
  }

  message(sprintf("Lese bestehende Shopify-Datenbank ein aus: %s", data_path))
  df_existing <- readRDS(data_path)

  # 2. Startdatum fuer den API-Call ermitteln
  if ("updated_at" %in% names(df_existing)) {
    # Da updated_at jetzt ein echtes dttm-Objekt (POSIXct) ist, koennen wir direkt damit rechnen
    max_date <- as.Date(max(df_existing$updated_at, na.rm = TRUE))

    # Overlap von 2 Tagen (DevOps Best Practice fuer nachtraegliche Webhook-Updates)
    start_fetch <- max_date - lubridate::days(2)
  } else {
    message("Konnte kein 'updated_at' finden. Lade die letzten 7 Tage als Fallback.")
    start_fetch <- Sys.Date() - lubridate::days(7)
  }

  # Shopify erwartet das Datum als ISO8601 String (z.B. 2024-03-21T00:00:00Z)
  updated_at_min <- format(start_fetch, "%Y-%m-%dT00:00:00Z")

  message(sprintf("Starte inkrementelles Update ab %s...", updated_at_min))

  # 3. Neue Daten von der API ziehen (ruft rohe Orders ab)
  # WICHTIG: Deine get_shopify_data muss den Parameter `updated_at_min` unterstuetzen!
  df_new_raw <- get_shopify_data(
    api_key = api_key,
    endpoint = endpoint,
    updated_at_min = updated_at_min
  )

  if (nrow(df_new_raw) == 0) {
    message("Keine neuen oder geupdateten Bestellungen gefunden. Datenbank ist aktuell.")
    return(invisible(TRUE))
  }

  message(sprintf("%s veränderte Bestellungen geladen. Starte Bereinigung und Item-Entpackung...", nrow(df_new_raw)))

  # 4. Neue Daten ins Item-Level-Format bringen (unsere neue Funktion)
  df_new_clean <- clean_up_shopify(df_new_raw)

  # RAM entlasten
  rm(df_new_raw)
  gc()

  # 5. Daten anhaengen und deduplizieren (Upsert)
  # WICHTIG: df_new_clean steht GANZ OBEN, damit distinct() die neusten Versionen behaelt!
  df_combined <- dplyr::bind_rows(df_new_clean, df_existing)

  if ("item_id" %in% names(df_combined)) {
    # Wir deduplizieren auf ITEM-Ebene, nicht auf Order-Ebene!
    df_combined <- df_combined |>
      dplyr::distinct(item_id, .keep_all = TRUE)
  } else {
    df_combined <- dplyr::distinct(df_combined)
  }

  # 6. Datenbank ueberschreiben
  saveRDS(df_combined, data_path)
  message(sprintf(
    "Upsert erfolgreich! %s neue/aktualisierte Items integriert. Gesamtbestand: %s Items.",
    nrow(df_new_clean), nrow(df_combined)
  ))

  return(invisible(TRUE))
}
