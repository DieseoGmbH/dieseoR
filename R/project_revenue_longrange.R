# =============================================================================
# project_revenue_longrange.R  —  Mehrjahres-Szenarien (dieseoR)
#
# ACHTUNG — anderes Werkzeug als forecast_revenue.R:
#   Kurzfrist (Tage/Wochen) = statistische Prognose (ETS, Intervalle).
#   Langfrist (Jahre)       = SZENARIO-PLANUNG. Kein Zeitreihenmodell kann
#   3-5 Jahre seriös punktprognostizieren. Das Ergebnis wird fast vollständig
#   von EINER Annahme bestimmt: wie schnell klingt das Wachstum ab?
#
# Warum keine naive Fortschreibung:
#   Pammys wächst aktuell ~+128% YoY. Konstant fortgeschrieben ergäbe das bis
#   2030 mehrere Milliarden € — für einen Schuhhändler unrealistisch. Deshalb
#   modellieren wir ABKLINGENDES Wachstum (Marktsättigung) und geben eine
#   Bandbreite (konservativ/basis/optimistisch) statt einer Scheinzahl aus.
#
#   get_annual_revenue()          Jahresumsätze (+ saisonale Hochrechnung lfd. Jahr)
#   project_revenue_scenarios()   Szenario-Fächer bis Zieljahr
# =============================================================================


#' Jahresumsätze inkl. saisonaler Hochrechnung des laufenden Jahres
#'
#' @title get_annual_revenue
#' @description Aggregiert die Tagesreihe zu Jahressummen. Das laufende
#'   (unvollständige) Jahr wird optional saisonal hochgerechnet: YTD des
#'   laufenden Jahres × (Vorjahr voll / Vorjahr gleicher Zeitraum). Das
#'   respektiert den Q4-Peak, anders als eine lineare Hochrechnung.
#' @param revenue_ts \code{tsibble} aus \code{get_daily_revenue()}. Default: intern.
#' @param annualize_current Laufendes Jahr saisonal hochrechnen? Default \code{TRUE}.
#' @return tibble mit \code{year}, \code{revenue}, \code{status}
#'   ("actual" / "partial" / "annualized").
#' @importFrom dplyr mutate group_by summarise filter pull bind_rows arrange
#' @importFrom lubridate year yday
#' @importFrom tibble as_tibble
#' @export
#' @examples
#' \dontrun{
#' get_annual_revenue()
#' }
get_annual_revenue <- function(revenue_ts = NULL, annualize_current = TRUE) {
  if (is.null(revenue_ts)) revenue_ts <- get_daily_revenue(drop_last_day = TRUE)

  # tsibble -> normales Tibble: sonst erzwingt tsibble die Index-Spalte `date`
  # bei group_by/summarise/bind_rows (Jahreszeilen haben kein `date`).
  df <- revenue_ts |>
    tibble::as_tibble() |>
    dplyr::mutate(
      year = lubridate::year(date),
      doy = lubridate::yday(date)
    )

  cur <- max(df$year)
  cutoff <- max(df$doy[df$year == cur]) # letzter erfasster Tag-des-Jahres
  full_yr <- max(df$doy[df$year == cur]) >= 365 # ist lfd. Jahr komplett?

  annual <- df |>
    dplyr::group_by(year) |>
    dplyr::summarise(revenue = sum(revenue), .groups = "drop") |>
    dplyr::mutate(status = "actual")

  if (!full_yr) {
    annual$status[annual$year == cur] <- "partial"
    if (isTRUE(annualize_current) && (cur - 1) %in% df$year) {
      ytd_cur <- sum(df$revenue[df$year == cur])
      ytd_prev <- sum(df$revenue[df$year == cur - 1 & df$doy <= cutoff])
      full_prev <- sum(df$revenue[df$year == cur - 1])
      if (ytd_prev > 0) {
        annual <- dplyr::bind_rows(
          annual,
          data.frame(
            year = cur, revenue = ytd_cur * (full_prev / ytd_prev),
            status = "annualized"
          )
        )
      }
    }
  }
  dplyr::arrange(annual, year, status)
}


#' Mehrjahres-Umsatzszenarien mit abklingendem Wachstum
#'
#' @title project_revenue_scenarios
#' @description Projiziert Jahresumsätze bis \code{to_year} unter Szenarien mit
#'   geometrisch abklingender YoY-Wachstumsrate:
#'   \deqn{g_t = g_\infty + (g_0 - g_\infty)\cdot decay^{t-1}}
#'   Kein statistisches Konfidenzintervall — die Bandbreite bildet
#'   \emph{Business-Annahmen} ab (Sättigungstempo), nicht Stichprobenrauschen.
#' @param base_value Umsatz des Ankerjahres (€). Default: aus
#'   \code{get_annual_revenue()} letztes volles Jahr.
#' @param base_year Ankerjahr. Default: letztes volles Jahr.
#' @param to_year Zieljahr. Default \code{2030}.
#' @param scenarios Benannte Liste; je Szenario \code{list(g0=, g_inf=, decay=)}.
#'   g0 = Start-YoY, g_inf = langfristige YoY, decay in (0,1) = Abkling-Tempo.
#'   Default: an aktueller ~+128%-Dynamik verankerte konservativ/basis/optimistisch.
#' @return tibble \code{scenario, year, revenue, growth} für alle Projektionsjahre.
#' @importFrom dplyr bind_rows
#' @export
#' @examples
#' \dontrun{
#' ann <- get_annual_revenue()
#' project_revenue_scenarios() # Defaults
#' }
project_revenue_scenarios <- function(
  base_value = NULL,
  base_year = NULL,
  to_year = 2030,
  scenarios = list(
    Konservativ  = list(g0 = 0.90, g_inf = 0.10, decay = 0.55),
    Basis        = list(g0 = 1.28, g_inf = 0.15, decay = 0.70),
    Optimistisch = list(g0 = 1.28, g_inf = 0.25, decay = 0.82)
  )
) {
  if (is.null(base_value) || is.null(base_year)) {
    ann <- get_annual_revenue()
    full <- ann[ann$status == "actual", , drop = FALSE]
    anchor <- full[which.max(full$year), ]
    if (is.null(base_value)) base_value <- anchor$revenue
    if (is.null(base_year)) base_year <- anchor$year
  }

  years <- (base_year + 1):to_year
  out <- lapply(names(scenarios), function(nm) {
    p <- scenarios[[nm]]
    v <- base_value
    rows <- lapply(seq_along(years), function(t) {
      g <- p$g_inf + (p$g0 - p$g_inf) * p$decay^(t - 1)
      v <<- v * (1 + g)
      data.frame(scenario = nm, year = years[t], revenue = v, growth = g)
    })
    do.call(rbind, rows)
  })
  do.call(rbind, out)
}
