#' @title Verschachtelte `requested_items` auf kompakte Spalten reduzieren
#' @description
#' Das Retourenportal liefert seit dem 02.09.2026 zusätzlich zum Order-Satz ein
#' verschachteltes Array `requested_items` mit den Feldern `id`, `item_type` und
#' `item_status`. Damit existieren zwei Status-Ebenen: der Order-Status in der
#' Spalte `status` und der Item-Status je Position.
#'
#' Das ist genau die Ebene, die für die Erstattungs-Logik des Fachbereichs
#' gebraucht wird:
#' \preformatted{
#'   items:  item_type = 'return' UND item_status = 'approved'
#'   order:  gift_card_offer = false
#'           UND status NOT IN ('requested','approved','refunded')
#' }
#'
#' Die Liste selbst wird **nicht** mitgespeichert: bei rund 474.000 Vorgängen
#' wären das ebenso viele verschachtelte Data-Frames im RDS — bei 16 GB RAM
#' weder nötig noch klug. Stattdessen entstehen hier drei flache Spalten, die
#' die Frage vollständig beantworten. Wird die Rohliste später doch gebraucht,
#' liegt sie in den Seiten-Chunks unter `raw_chunks/`.
#'
#' Fehlt `requested_items` in der Antwort (ältere Bestände, ausgefallenes
#' Feld), werden die Spalten mit `NA` angelegt statt zu scheitern — sonst
#' bräche jeder Merge mit Altdaten an unterschiedlichen Spaltensätzen.
#'
#' @param df Data Frame einer API-Seite (`parsed$data$data`).
#' @return Derselbe Data Frame ohne `requested_items`, dafür mit
#'   `items_n`, `items_return_approved` und `item_status_set`.
#' @export
#' @examples
#' \dontrun{
#' parsed <- jsonlite::fromJSON(txt, flatten = TRUE)
#' d <- summarise_requested_items(parsed$data$data)
#' }
summarise_requested_items <- function(df) {
  if (!is.data.frame(df) || nrow(df) == 0) {
    return(df)
  }

  if (!"requested_items" %in% names(df)) {
    df$items_n <- NA_integer_
    df$items_return_approved <- NA_integer_
    df$item_status_set <- NA_character_
    return(df)
  }

  it <- df$requested_items
  # Der API-Parser liefert je Zeile ein data.frame; bei leerem Array kann
  # auch NULL oder eine leere Liste kommen -> beides abfangen.
  ok <- function(x) {
    is.data.frame(x) && nrow(x) > 0 &&
      all(c("item_type", "item_status") %in% names(x))
  }

  df$items_n <- vapply(it, function(x) {
    if (is.data.frame(x)) nrow(x) else 0L
  }, integer(1))

  # Die eine Zahl, um die es geht: Positionen vom Typ 'return', die
  # freigegeben sind und deren Geld damit aussteht.
  df$items_return_approved <- vapply(it, function(x) {
    if (!ok(x)) {
      return(0L)
    }
    sum(x$item_type == "return" & x$item_status == "approved", na.rm = TRUE)
  }, integer(1))

  # Alle vorkommenden Item-Status als sortierte Liste — nützlich zum
  # Nachvollziehen, warum ein Vorgang in eine Kennzahl fällt oder nicht.
  df$item_status_set <- vapply(it, function(x) {
    if (!ok(x)) {
      return(NA_character_)
    }
    paste(sort(unique(x$item_status)), collapse = ",")
  }, character(1))

  df$requested_items <- NULL
  df
}
