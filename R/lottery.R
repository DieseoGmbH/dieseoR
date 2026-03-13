#' Ziehung der Gewinner eines Gewinnspiels
#'
#' Diese Funktion führt eine gewichtete Ziehung von Gewinnern anhand der Anzahl der Lose durch
#' und weist zufällig Preise zu.
#'
#' @param data Ein Dataframe mit mindestens den Spalten: id, email, first_name, last_name, amount_lose.
#' @param prizes Ein Vektor mit den zu vergebenden Preisen (z.B. von \code{rep()} erzeugt). Default: vordefinierte Preise.
#' @param seed Optional. Zufalls-Seed für Reproduzierbarkeit. Default ist NULL (kein gesetzter Seed).
#'
#' @return Ein Dataframe mit den Gewinnern und den zugewiesenen Preisen.
#' @examples
#' winners <- lottery(gewinnspiel_filtered, seed = 123)
lottery <- function(
  data,
  prizes = c(
    rep("Reisegutschein 2.500 €", 10),
    rep("Erstattete Bestellung", 100),
    rep("Gratis Produkt neue Kollektion", 100),
    rep("50 € Pammys Gutschein", 300),
    rep("Luxus-Wellness-Wochenende (Wellcard 500 €)", 10),
    rep("Spa- & Beauty-Tag (Wellcard 300 €)", 10),
    rep("Shopping-Day 1.000 €", 5)
  ),
  seed = NULL
) {
  if (!is.null(seed)) set.seed(seed)

  winners <- data |>
    dplyr::filter(amount_lose > 0) |>
    dplyr::slice_sample(n = length(prizes), weight_by = amount_lose, replace = FALSE) |>
    dplyr::mutate(prize = sample(prizes)) |>
    dplyr::select(id, email, first_name, last_name, amount_lose, prize)

  winners
}
