#' @title Inkrementelles Update der Shopify Daten (Upsert)
#'
#' @description Zieht geaenderte Daten via API, entpackt diese dynamisch je nach
#' Endpunkt und fuehrt einen deduplizierten Upsert durch. Memory-Safe durch Auto-Chunking.
#'
#' @param datadir Character. Der Pfad zum zentralen Datenverzeichnis (`~/data/`).
#' @param endpoint Character. Welcher Endpunkt aktualisiert werden soll ("orders", "checkouts", "products", "customers").
#' @param api_key Character. Das Shopify Admin API Access Token.
#' @param data_path Optional. Manueller Pfad zur RDS-Datei. Wird sonst automatisch berechnet.
#'
#' @return Invisible TRUE bei Erfolg.
#' @export
#'
#' @importFrom dplyr bind_rows distinct filter arrange desc
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

  # 3. Startdatum für den API-Call ermitteln
  if ("updated_at" %in% names(df_existing)) {
    max_date <- as.Date(max(df_existing$updated_at, na.rm = TRUE))
    # FIX: 100 Tage zurückblicken, um späte Stornos/Refunds sicher abzugreifen
    start_fetch <- max_date - lubridate::days(100)
  } else {
    message("Konnte kein 'updated_at' finden. Lade die letzten 100 Tage als Fallback.")
    start_fetch <- Sys.Date() - lubridate::days(100)
  }

  updated_at_min <- format(start_fetch, "%Y-%m-%dT00:00:00Z")
  message(sprintf("Starte inkrementelles Update für '%s' ab %s...", endpoint, updated_at_min))

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

  message(sprintf("%s veränderte Datensaetze geladen. Starte speicherschonende Bereinigung...", nrow(df_new_raw)))

  # ----------------------------------------------------------------------------
  # 5. MEMORY-SAFE BEREINIGUNG (CHUNK-BASED)
  # ----------------------------------------------------------------------------
  chunk_size <- 2500 # Bei 2500 Orders pro Chunk bläht der RAM nicht zu stark auf
  n_rows <- nrow(df_new_raw)

  if (n_rows > chunk_size) {
    # Daten in eine Liste von kleineren Dataframes splitten
    raw_chunks <- split(df_new_raw, ceiling(seq_len(n_rows) / chunk_size))
    cleaned_chunks <- list()

    for (i in seq_along(raw_chunks)) {
      message(sprintf("  -> Bereinige Chunk %d von %d...", i, length(raw_chunks)))

      clean_part <- clean_up_shopify(raw_chunks[[i]], endpoint = endpoint)
      cleaned_chunks[[i]] <- clean_part

      # RAM direkt wieder freigeben
      raw_chunks[[i]] <- NULL
      gc()
    }

    # Wieder zusammenkleben
    df_new_clean <- dplyr::bind_rows(cleaned_chunks)
    rm(cleaned_chunks)
    gc()
  } else {
    # Wenn es ohnehin wenige Daten sind, normal ausführen
    df_new_clean <- clean_up_shopify(df_new_raw, endpoint = endpoint)
  }

  # Rohe Daten endgültig löschen
  rm(df_new_raw)
  gc()

  # ----------------------------------------------------------------------------
  # 6. Daten anhaengen und deduplizieren
  # ----------------------------------------------------------------------------
  df_combined <- dplyr::bind_rows(df_new_clean, df_existing)

  # 7. Dynamische Deduplizierung je nach Endpunkt (inkl. STRKTER ID- & Datum-Sortierung!)
  df_combined <- switch(endpoint,
    "orders" = df_combined |>
      dplyr::arrange(order_id, item_id, dplyr::desc(updated_at)) |>
      dplyr::distinct(item_id, .keep_all = TRUE),
    "products" = df_combined |>
      dplyr::arrange(variants_id, dplyr::desc(updated_at)) |>
      dplyr::distinct(variants_id, .keep_all = TRUE),
    "customers" = df_combined |>
      dplyr::arrange(id, dplyr::desc(updated_at)) |>
      dplyr::distinct(id, .keep_all = TRUE),
    "checkouts" = df_combined |>
      dplyr::arrange(order_id, item_id, dplyr::desc(updated_at)) |>
      dplyr::distinct(order_id, item_id, .keep_all = TRUE),

    # Fallback
    df_combined |>
      dplyr::arrange(dplyr::desc(updated_at)) |>
      dplyr::distinct()
  )

  # 8. Datenbank ueberschreiben
  saveRDS(df_combined, data_path)
  message(sprintf(
    "Upsert erfolgreich! %s neue/aktualisierte '%s' integriert. Gesamtbestand: %s.",
    nrow(df_new_clean), endpoint, nrow(df_combined)
  ))

  return(invisible(TRUE))
}
