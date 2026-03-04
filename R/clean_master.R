#' Lese Daten ein und führe Standard-Säuberungen durch
#'
#' Diese Funktion nimmt einen Dateipfad, liest den entsprechenden Datensatz
#' (RDS, CSV oder Excel) ein und führt direkt Standard-Bereinigungen durch.
#' Die Spaltennamen werden mittels \code{janitor::clean_names()} standardisiert.
#' Zudem werden alle Textspalten (Character) von überschüssigen Leerzeichen
#' befreit und in Kleinbuchstaben umgewandelt.
#'
#' @param input Character. Der komplette Pfad zur Datei, die eingelesen werden soll.
#' @param type Character. Das Format der Datei. Unterstützt werden "rds" (Standard), "csv", "excel" (oder "xlsx"/"xls").
#' @param ... Weitere Argumente, die an die jeweilige Einlese-Funktion (z. B. \code{read.csv} oder \code{readxl::read_excel}) weitergegeben werden sollen.
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
#' # RDS-Datei einlesen
#' df_rds <- clean_master("data/meine_daten.rds")
#'
#' # CSV-Datei einlesen (mit base R utils::read.csv)
#' df_csv <- clean_master("data/rohdaten.csv", type = "csv", sep = ";", dec = ",")
#'
#' # Excel-Datei einlesen (liest nur das Tabellenblatt "Umsatz")
#' df_excel <- clean_master("data/finanzen.xlsx", type = "excel", sheet = "Umsatz")
#' }
clean_master <- function(input, type = "rds", ...) {

  # 1. Um Fehler abzufangen, falls jemand "CSV" groß schreibt
  file_type <- tolower(type)

  # 2. Datei einlesen, abhängig vom Typ
  if (file_type == "rds") {
    df <- readRDS(input)

  } else if (file_type == "csv") {
    # Nutzt base R (utils) anstelle von readr
    df <- utils::read.csv(input, ...)

  } else if (file_type %in% c("excel", "xlsx", "xls")) {
    # Benötigt weiterhin das readxl-Package
    df <- readxl::read_excel(input, ...)

  } else {
    stop("Nicht unterstützter Dateityp. Bitte 'rds', 'csv' oder 'excel' verwenden.")
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

  return(df_clean)
}
