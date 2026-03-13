#' Lese Daten ein und/oder führe Standard-Säuberungen durch
#'
#' Diese Funktion nimmt entweder einen Dateipfad oder direkt einen Data Frame.
#' Wenn ein Pfad übergeben wird, liest sie den entsprechenden Datensatz
#' (RDS, CSV oder Excel) ein. Anschließend führt sie direkte Standard-Bereinigungen durch:
#' Die Spaltennamen werden mittels \code{janitor::clean_names()} standardisiert.
#' Zudem werden alle Textspalten (Character) von überschüssigen Leerzeichen
#' befreit und in Kleinbuchstaben umgewandelt.
#'
#' @param input Character oder Data Frame. Der komplette Pfad zur Datei, die eingelesen werden soll, oder ein bereits geladener Data Frame.
#' @param type Character. Das Format der Datei, falls ein Pfad übergeben wird. Unterstützt werden "rds" (Standard), "csv", "excel" (oder "xlsx"/"xls").
#' @param ... Weitere Argumente, die an die jeweilige Einlese-Funktion (z. B. \code{utils::read.csv} oder \code{readxl::read_excel}) weitergegeben werden sollen.
#'
#' @return Ein bereinigter Data Frame (bzw. Tibble).
#' @export
#'
#' @importFrom janitor clean_names
#' @importFrom dplyr mutate across
#' @importFrom tidyselect where
#' @importFrom stringr str_trim
#' @importFrom utils read.csv
#'
#' @examples
#' \dontrun{
#' # 1. RDS-Datei einlesen und bereinigen
#' df_rds <- clean_master("data/meine_daten.rds")
#'
#' # 2. Bereits geladenen Data Frame nur bereinigen
#' mein_df <- data.frame(Name_Spalte = c("  Test ", "HALLO  "))
#' df_clean <- clean_master(mein_df)
#' }
clean_master <- function(input, type = "rds", ...) {
  # 1. Prüfen, ob input ein Data Frame oder ein Dateipfad ist
  if (is.data.frame(input)) {
    # Wenn es schon ein Data Frame ist, einfach übernehmen und Einlesen überspringen
    df <- input
  } else if (is.character(input)) {
    # 2. Wenn es ein Pfad ist, Datei einlesen abhängig vom Typ
    file_type <- tolower(type)

    if (file_type == "rds") {
      df <- readRDS(input)
    } else if (file_type == "csv") {
      # Nutzt base R (utils) anstelle von readr
      df <- utils::read.csv(input, ...)
    } else if (file_type %in% c("excel", "xlsx", "xls")) {
      # Benötigt weiterhin das readxl-Package
      if (!requireNamespace("readxl", quietly = TRUE)) stop("Bitte installiere das 'readxl' Paket.")
      df <- readxl::read_excel(input, ...)
    } else {
      stop("Nicht unterstützter Dateityp. Bitte 'rds', 'csv' oder 'excel' verwenden.")
    }
  } else {
    stop("'input' muss entweder ein Dateipfad (Character) oder ein Data Frame sein.")
  }

  # 3. Die Datenbereinigung
  df_clean <- df |>
    janitor::clean_names() |>
    dplyr::mutate(
      dplyr::across(
        tidyselect::where(is.character),
        ~ tolower(stringr::str_trim(.x))
      )
    )

  df_clean
}
