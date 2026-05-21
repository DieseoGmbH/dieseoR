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
    rep("price1", 10),
    rep("price2", 100)
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
