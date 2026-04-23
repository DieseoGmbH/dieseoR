#' @title Aktualisiere Meta Ads Masterdatei inkrementell (Delta-Update)
#' @description Liest die Masterdatei (`meta_daily_request.rds` oder
#' `meta_hourly_request.rds`) aus `~/data/meta/`, ermittelt das höchste Datum
#' (aus `date_start` bzw. `date`) und zieht nur den fehlenden Zeitraum über
#' `get_meta_data()` nach. Die letzten `overlap_days` werden erneut geladen
#' und im Master überschrieben, da Meta Spend-Daten mit 24–48h Verzug
#' finalisiert.
#' @param granularity `"daily"` oder `"hourly"`.
#' @param end_date Enddatum der Aktualisierung (Default: `Sys.Date()`).
#' @param overlap_days Tage ab Master-Ende, die erneut gezogen und überschrieben
#' werden (Default 2).
#' @param datadir Basis-Datenordner. Default: `datadir` aus `.GlobalEnv`
#' (gesetzt via `~/workspace/local.R`).
#' @return Unsichtbar: der aktualisierte Master-Datensatz als `tibble`.
#' @family adribute
#' @importFrom dplyr filter bind_rows distinct arrange
#' @importFrom rlang .data
#' @export
#' @examples
#' \dontrun{
#' update_meta_data("daily")
#' update_meta_data("hourly", overlap_days = 3)
#' }
update_meta_data <- function(granularity = c("daily", "hourly"),
                             end_date = Sys.Date(),
                             overlap_days = 2,
                             datadir = NULL,
                             token = Sys.getenv("META_ACCESS_TOKEN"),
                             ad_acc_id = Sys.getenv("META_AD_ACCOUNT_ID")) {
  granularity <- match.arg(granularity)

  if (is.null(datadir)) {
    if (!exists("datadir", envir = .GlobalEnv)) {
      stop(
        "`datadir` nicht gefunden. Bitte `source('~/workspace/local.R')` ausführen ",
        "oder `datadir` explizit übergeben."
      )
    }
    datadir <- get("datadir", envir = .GlobalEnv)
  }

  # Schema-Konfiguration je Granularität
  cfg <- switch(granularity,
    daily  = list(file = "meta_daily_request.rds", date_col = "date_start"),
    hourly = list(file = "meta_hourly_request.rds", date_col = "date")
  )
  master_path <- file.path(datadir, "meta", cfg$file)

  if (!file.exists(master_path)) {
    stop(
      "Masterdatei nicht gefunden: ", master_path, "\n",
      "Bitte zuerst initialen Vollabzug erzeugen, z.B.:\n",
      '  get_meta_data("2024-01-01", Sys.Date(), granularity = "', granularity, '") |>\n',
      '    saveRDS(file.path(datadir, "meta/', cfg$file, '"))'
    )
  }

  master <- readRDS(master_path)
  if (!cfg$date_col %in% names(master) || nrow(master) == 0) {
    stop("Masterdatei enthält keine Spalte `", cfg$date_col, "` oder ist leer: ", master_path)
  }

  last_date <- max(master[[cfg$date_col]], na.rm = TRUE)
  end_date <- as.Date(end_date)
  start_date <- last_date - overlap_days

  if (start_date > end_date) {
    message(
      "Masterdatei bereits aktuell (letztes Datum: ", last_date,
      ", Enddatum: ", end_date, "). Nichts zu tun."
    )
    return(invisible(master))
  }

  message(
    "Meta-Delta-Update [", granularity, "]: ", start_date, " bis ", end_date,
    " (Master-Stand: ", last_date, ", Overlap: ", overlap_days, " Tage)."
  )

  new_data <- get_meta_data(start_date, end_date,
    granularity = granularity,
    token = token,
    ad_account_id = ad_acc_id
  )

  if (nrow(new_data) == 0) {
    message("Keine neuen Daten von Meta erhalten. Masterdatei unverändert.")
    return(invisible(master))
  }

  # Overlap-Bereich aus Master entfernen, neue Daten anhängen, deduplizieren
  combined <- master |>
    dplyr::filter(.data[[cfg$date_col]] < start_date) |>
    dplyr::bind_rows(new_data) |>
    dplyr::distinct()

  combined <- if ("hour" %in% names(combined)) {
    combined |> dplyr::arrange(.data[[cfg$date_col]], as.integer(hour), ad_id)
  } else {
    combined |> dplyr::arrange(.data[[cfg$date_col]], ad_id)
  }

  saveRDS(combined, master_path)

  message(
    "Master '", cfg$file, "' aktualisiert: ",
    nrow(combined), " Zeilen gesamt (", nrow(new_data), " neu geladen, ",
    "Min: ", min(combined[[cfg$date_col]]), ", Max: ", max(combined[[cfg$date_col]]), ")."
  )

  invisible(combined)
}
