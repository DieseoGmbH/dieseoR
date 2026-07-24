# =============================================================================
# forecast_revenue.R  —  Aggregierte Umsatzprognose (dieseoR)
#
# Funktionen, aufeinander aufbauend:
#   get_daily_revenue()          Extraktion + Bereinigung der Tagesumsatzreihe
#   build_revenue_forecast()     Fit + Prognose (Harmonic-ARIMA ODER ETS)
#   backtest_revenue_forecast()  Rolling-Origin-Genauigkeit (MAE/MAPE/RMSE/
#                                sumAPE + Intervall-Coverage), Modell-Bake-off
#   calibrate_revenue_intervals()Conformal-Ratios aus Backtest (ehrliche Bänder)
#   summarise_revenue_forecast() Aggregat-Sicht (Woche/Monat/Horizont-Total)
#
# Design-Entscheidungen (aus Bake-off auf echten Daten, 27.05.2022-21.07.2026,
#   9 Rolling-Folds à h=28, zurück bis Black Friday 2025):
#   * Reihe hat ZWEI Saisonalitäten: Woche (7) UND Jahr (~4,8x Swing Feb->Nov,
#     Sommer-/Black-Friday-Doppelpeak). Ein reines Wochen-ETS ist strukturell
#     blind fürs Jahr und lag im Winter 160-300% daneben -> es hat den Backtest
#     VERLOREN (schlechter als Seasonal-Naive). Deshalb DEFAULT = "harmonic":
#     ARIMA(log) + fourier(Woche) + fourier(Jahr). Halbiert MAE ggü. Alt-ETS.
#   * Wachstum stark & nicht-stationär -> log-Transform (multiplikativ) UND
#     Differenzierung d=1 (pdq(1,1,1)): trägt den Wachstumstrend mit. Ohne d=1
#     (d=0) revertiert das log-Level zum jüngsten Mittel und unterschätzt die
#     Horizont-Summe systematisch um ~49% (Backtest-Bias 1,49). Mit d=1 ist die
#     Prognose praktisch unverzerrt (Bias 0,91) und sumAPE fällt 37%->27%.
#   * Jahressaison braucht >=2 Zyklen -> train_days Default 1095 (3 Jahre); die
#     dünne 2022er-Startphase fällt damit sinnvoll raus.
#   * Tagesumsatz ist extrem schwerschwänzig (Promo-Spitzen, Actual 0,4x-5,4x
#     der Prognose): analytische Modell-Intervalle sind zu eng (Coverage 67%
#     statt 80%). -> optionale CONFORMAL-Rekalibrierung aus dem Backtest.
#   * Einzeltag ist kaum punktgenau -> fürs Planen die AGGREGAT-Sicht nutzen
#     (Woche/Monat/Total), dort mitteln sich Fehler heraus (sumAPE ~34%).
#   * Aktueller Tag ist bei nächtlicher Extraktion unvollständig -> drop_last_day.
#
# ANNAHME (bitte fachlich bestätigen): "Umsatz" = SUM(item_gross_revenue) je
#   created_at-Tag über NICHT stornierte Positionen (cancellation_status = FALSE),
#   brutto (Retouren/Refunds NICHT abgezogen). Für Netto siehe build_daily_business().
# =============================================================================


#' Tägliche Shopify-Umsatzreihe aus DuckDB extrahieren
#'
#' @title get_daily_revenue
#' @description Aggregiert den Tagesumsatz direkt in DuckDB (RAM-sicher, keine
#'   Rohzeilen in den R-Prozess) und liefert eine lückenlose, bereinigte
#'   Tagesreihe als \code{tsibble}. Aggregation und Filter laufen in der DB.
#' @param duckdb_path Pfad zur DuckDB. Default: Dashboard-Data-Mart.
#' @param table Name der Orders-Tabelle. Default \code{"orders"}.
#' @param drop_last_day Letzten Kalendertag verwerfen (bei nächtlicher
#'   Extraktion unvollständig). Default \code{TRUE}.
#' @param min_date Optionaler Startschnitt (Date oder "YYYY-MM-DD"); frühe,
#'   dünne Historie kann die Modelle stören. Default \code{NULL} (alles).
#' @return \code{tsibble} mit Spalten \code{date} (Index, Tag) und
#'   \code{revenue} (numeric). Fehlende Tage sind gefüllt und interpoliert.
#' @importFrom DBI dbConnect dbGetQuery dbDisconnect dbQuoteIdentifier
#' @importFrom duckdb duckdb
#' @importFrom dplyr mutate filter arrange
#' @importFrom tsibble as_tsibble fill_gaps
#' @importFrom tidyr fill
#' @export
#' @examples
#' \dontrun{
#' rev <- get_daily_revenue()
#' print(rev)
#' }
get_daily_revenue <- function(
  duckdb_path = "~/git/dashboard/data/shopify.duckdb",
  table = "orders",
  drop_last_day = TRUE,
  min_date = NULL
) {
  con <- DBI::dbConnect(duckdb::duckdb(),
    dbdir = path.expand(duckdb_path), read_only = TRUE
  )
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Aggregation in der DB -> nur ~1.500 Tageszeilen kommen im RAM an
  sql <- sprintf("
    SELECT CAST(created_at AS DATE) AS date,
           SUM(item_gross_revenue)  AS revenue
    FROM %s
    WHERE cancellation_status = FALSE
      AND created_at IS NOT NULL
    GROUP BY 1
    ORDER BY 1", DBI::dbQuoteIdentifier(con, table))
  daily <- DBI::dbGetQuery(con, sql)

  daily <- daily |>
    dplyr::mutate(date = as.Date(date)) |>
    dplyr::arrange(date)

  if (!is.null(min_date)) {
    daily <- dplyr::filter(daily, date >= as.Date(min_date))
  }
  if (isTRUE(drop_last_day)) {
    daily <- dplyr::filter(daily, date < max(date))
  }

  # Lückenlose Tageachse + fehlende Tage interpolieren (Set-Semantik: keine Löcher)
  ts <- daily |>
    tsibble::as_tsibble(index = date) |>
    tsibble::fill_gaps() |>
    tidyr::fill(revenue, .direction = "downup")

  ts$revenue <- pmax(ts$revenue, 1) # >0 für multiplikatives Modell / log
  ts
}


# --- intern: Modellformel je Methode -----------------------------------------
# Gibt eine benannte fable-Modellspezifikation zurück. Nicht exportiert.
.revenue_model_spec <- function(method = c("harmonic", "ets", "snaive"),
                                fourier_week_K = 3, fourier_year_K = 4) {
  method <- match.arg(method)
  switch(method,
    harmonic = fable::ARIMA(
      log(revenue) ~
        fourier(period = 7, K = fourier_week_K) +
        fourier(period = 365.25, K = fourier_year_K) +
        PDQ(0, 0, 0) + pdq(1, 1, 1),
      stepwise = TRUE, approximation = TRUE
    ),
    ets = fable::ETS(revenue ~ error("M") + trend("Ad") + season("M")),
    snaive = fable::SNAIVE(revenue ~ lag("week"))
  )
}


#' Umsatzprognose bauen (Harmonic-ARIMA mit Wochen- & Jahressaison, Default)
#'
#' @title build_revenue_forecast
#' @description Trainiert auf dem jüngsten Fenster ein Prognosemodell und liefert
#'   eine \code{h}-Tage-Prognose inkl. 80\%/95\%-Intervallen als tidy tibble
#'   (dashboard-fertig). \strong{Default} \code{method="harmonic"}: dynamische
#'   harmonische Regression \code{ARIMA(log(revenue) ~ fourier(Woche) +
#'   fourier(Jahr))} — bildet Wochen- UND Jahressaison ab und gewann den
#'   Rolling-Origin-Backtest deutlich (halber MAE ggü. dem alten Wochen-ETS,
#'   das im Winter strukturell versagte). \code{method="ets"} reproduziert das
#'   alte multiplikative ETS (nur zum Vergleich). Werden \code{conformal}-Ratios
#'   übergeben (aus \code{calibrate_revenue_intervals()}), ersetzen sie die zu
#'   engen analytischen Intervalle durch empirisch kalibrierte Bänder.
#' @param revenue_ts \code{tsibble} aus \code{get_daily_revenue()}. Default:
#'   wird intern frisch geladen.
#' @param h Prognosehorizont in Tagen. Default \code{28}.
#' @param method Modell: \code{"harmonic"} (Default), \code{"ets"} oder
#'   \code{"snaive"}.
#' @param train_days Länge des Trainingsfensters in Tagen. Default \code{1095}
#'   (3 Jahre; harmonic braucht >=2 Jahressaison-Zyklen). \code{NULL} = alles.
#' @param conformal Optionale Conformal-Ratios aus
#'   \code{calibrate_revenue_intervals()} (Liste mit \code{$daily}). Ersetzen
#'   die Modell-Intervalle durch \code{.mean * Ratio-Quantil}. Default \code{NULL}.
#' @param fourier_week_K,fourier_year_K Anzahl Fourier-Terme (nur harmonic).
#'   Defaults 3 bzw. 4.
#' @param model_spec Optionale abweichende \code{fable}-Modellformel (überschreibt
#'   \code{method}).
#' @return tibble mit \code{date}, \code{.mean} (Punktprognose),
#'   \code{lo80, hi80, lo95, hi95}, \code{.model} und \code{.interval}
#'   ("model" oder "conformal").
#' @importFrom dplyr filter
#' @importFrom fabletools model forecast hilo
#' @importFrom fable ARIMA ETS SNAIVE
#' @importFrom stats quantile
#' @importFrom tibble tibble
#' @export
#' @examples
#' \dontrun{
#' fc <- build_revenue_forecast(h = 28) # harmonic
#' cal <- calibrate_revenue_intervals()
#' fc <- build_revenue_forecast(h = 28, conformal = cal) # kalibriert
#' }
build_revenue_forecast <- function(
  revenue_ts = NULL,
  h = 28,
  method = c("harmonic", "ets", "snaive"),
  train_days = 1095,
  conformal = NULL,
  fourier_week_K = 3,
  fourier_year_K = 4,
  model_spec = NULL
) {
  method <- match.arg(method)
  if (is.null(revenue_ts)) revenue_ts <- get_daily_revenue()

  train <- revenue_ts
  if (!is.null(train_days)) {
    train <- dplyr::filter(train, date > max(date) - train_days)
  }
  if (method == "harmonic" && nrow(train) < 730) {
    warning(
      "harmonic braucht >=2 Jahre Training fuer die Jahressaison; ",
      "nur ", nrow(train), " Tage vorhanden. Ergebnis unsicher."
    )
  }

  spec <- model_spec %||%
    .revenue_model_spec(method, fourier_week_K, fourier_year_K)

  fit <- fabletools::model(train, m = spec)

  # Fallback, falls die ARIMA-Suche kein stabiles Modell findet (Pipeline-Schutz)
  if (is.null(fit[[1]][[1]]) && method == "harmonic") {
    warning("harmonic-Fit fehlgeschlagen -> Fallback auf ETS.")
    method <- "ets"
    fit <- fabletools::model(train, m = .revenue_model_spec("ets"))
  }

  fc <- fabletools::forecast(fit, h = h)
  iv80 <- fabletools::hilo(fc, level = 80)$`80%`
  iv95 <- fabletools::hilo(fc, level = 95)$`95%`

  mdl_label <- c(
    harmonic = "Harmonic-ARIMA(log, Woche+Jahr)",
    ets = "ETS(M,Ad,M)", snaive = "SNAIVE(Woche)"
  )[method]

  out <- tibble::tibble(
    date      = fc$date,
    .mean     = as.numeric(fc$.mean),
    lo80      = iv80$lower, hi80 = iv80$upper,
    lo95      = iv95$lower, hi95 = iv95$upper,
    .model    = unname(mdl_label),
    .interval = "model"
  )

  # Conformal: analytische Bänder durch empirisch kalibrierte ersetzen
  if (!is.null(conformal) && !is.null(conformal$daily)) {
    q <- conformal$daily
    out$lo80 <- out$.mean * q[["p10"]]
    out$hi80 <- out$.mean * q[["p90"]]
    out$lo95 <- out$.mean * q[["p025"]]
    out$hi95 <- out$.mean * q[["p975"]]
    out$.interval <- "conformal"
  }
  out
}


#' Rolling-Origin-Backtest & Modell-Bake-off der Umsatzprognose
#'
#' @title backtest_revenue_forecast
#' @description Verschiebt den Prognose-Ursprung in \code{h}-Schritten zurück,
#'   prognostiziert jeweils \code{h} Tage und misst gegen die Realität. Neben
#'   MAE/MAPE/RMSE werden \code{sumAPE} (Fehler der Horizont-SUMME — die
#'   planungsrelevante Größe) und die tatsächliche Intervall-Abdeckung
#'   (\code{cov80/cov95}) berechnet. So bleibt belegbar, welches Modell wirklich
#'   gewinnt und ob die Bänder halten, was sie versprechen.
#' @param revenue_ts \code{tsibble} aus \code{get_daily_revenue()}. Default: intern.
#' @param h Horizont je Fold (Tage). Default \code{28}.
#' @param n_folds Anzahl rollierender Ursprünge. Default \code{9}.
#' @param methods Zu vergleichende Modelle. Default alle drei.
#' @param train_days Trainingsfenster je Fold (Tage). Default \code{1095}.
#' @param return_preds Auch die Tages-Vorhersagen zurückgeben (für Conformal)?
#'   Default \code{FALSE}.
#' @return tibble je Fold & Methode: \code{origin, .model, mae, mape, rmse,
#'   sumape, cov80, cov95}. Bei \code{return_preds=TRUE} eine Liste
#'   \code{list(metrics=, preds=)}.
#' @importFrom dplyr filter mutate inner_join bind_rows group_by summarise
#' @importFrom fabletools model forecast hilo
#' @importFrom fable ARIMA ETS SNAIVE
#' @importFrom tsibble as_tibble
#' @importFrom tibble tibble
#' @export
#' @examples
#' \dontrun{
#' bt <- backtest_revenue_forecast(n_folds = 9)
#' dplyr::group_by(bt, .model) |>
#'   dplyr::summarise(dplyr::across(c(mae, mape, sumape, cov80), mean))
#' }
backtest_revenue_forecast <- function(
  revenue_ts = NULL,
  h = 28,
  n_folds = 9,
  methods = c("harmonic", "ets", "snaive"),
  train_days = 1095,
  return_preds = FALSE
) {
  if (is.null(revenue_ts)) revenue_ts <- get_daily_revenue()
  max_d <- max(revenue_ts$date)

  metrics <- list()
  preds <- list()
  for (k in seq_len(n_folds)) {
    origin <- max_d - (k - 1L) * h
    test <- dplyr::filter(revenue_ts, date > origin, date <= origin + h)
    if (nrow(test) < h) next
    train <- dplyr::filter(
      revenue_ts, date <= origin,
      date > origin - (train_days %||% 1e6)
    )

    for (mth in methods) {
      # ETS/SNAIVE brauchen kein langes Fenster -> kürzer & schneller halten
      tr <- if (mth == "harmonic") {
        train
      } else {
        dplyr::filter(revenue_ts, date <= origin, date > origin - 400)
      }
      fit <- tryCatch(fabletools::model(tr, m = .revenue_model_spec(mth)),
        error = function(e) NULL
      )
      if (is.null(fit) || is.null(fit[[1]][[1]])) next
      fc <- tryCatch(fabletools::forecast(fit, h = h), error = function(e) NULL)
      if (is.null(fc)) next

      iv80 <- fabletools::hilo(fc, 80)$`80%`
      iv95 <- fabletools::hilo(fc, 95)$`95%`
      d <- tibble::tibble(
        date = fc$date, mean = as.numeric(fc$.mean),
        l80 = iv80$lower, u80 = iv80$upper,
        l95 = iv95$lower, u95 = iv95$upper,
        hstep = seq_len(nrow(fc))
      ) |>
        dplyr::inner_join(tibble::as_tibble(test)[, c("date", "revenue")], by = "date")
      if (nrow(d) < h) next

      metrics[[length(metrics) + 1]] <- tibble::tibble(
        origin = origin, .model = mth,
        mae = mean(abs(d$mean - d$revenue)),
        mape = mean(abs(d$mean - d$revenue) / d$revenue) * 100,
        rmse = sqrt(mean((d$mean - d$revenue)^2)),
        sumape = abs(sum(d$mean) - sum(d$revenue)) / sum(d$revenue) * 100,
        cov80 = mean(d$revenue >= d$l80 & d$revenue <= d$u80) * 100,
        cov95 = mean(d$revenue >= d$l95 & d$revenue <= d$u95) * 100
      )
      if (return_preds) {
        preds[[length(preds) + 1]] <- dplyr::mutate(d, .model = mth, origin = origin)
      }
    }
  }
  m <- dplyr::bind_rows(metrics)
  if (return_preds) list(metrics = m, preds = dplyr::bind_rows(preds)) else m
}


#' Conformal-Kalibrierung der Prognose-Intervalle aus dem Backtest
#'
#' @title calibrate_revenue_intervals
#' @description Leitet ehrliche Prognose-Intervalle empirisch aus dem
#'   Rolling-Origin-Backtest ab (multiplikative Conformal-Kalibrierung). Sammelt
#'   die Verhältnisse \code{Ist / Prognose} und nimmt deren Quantile — für
#'   Tageswerte \emph{und} für die Horizont-Summe getrennt (letztere ist viel
#'   enger, weil sich Tagesfehler herausmitteln). Nötig, weil die analytischen
#'   ARIMA-Bänder für diese schwerschwänzige Reihe zu eng sind (nur ~67\%
#'   statt 80\% Abdeckung).
#' @param revenue_ts \code{tsibble} aus \code{get_daily_revenue()}. Default: intern.
#' @param method Modell, dessen Intervalle kalibriert werden. Default "harmonic".
#' @param h,n_folds,train_days wie in \code{backtest_revenue_forecast()}.
#' @return Liste mit \code{$daily} und \code{$horizon} (je benannter Vektor mit
#'   \code{p025,p10,p90,p975}) plus \code{$n_folds}. \code{$daily} geht direkt in
#'   \code{build_revenue_forecast(conformal = )}, \code{$horizon} in
#'   \code{summarise_revenue_forecast()}.
#' @importFrom stats quantile
#' @importFrom dplyr group_by summarise
#' @export
#' @examples
#' \dontrun{
#' cal <- calibrate_revenue_intervals()
#' build_revenue_forecast(conformal = cal)
#' }
calibrate_revenue_intervals <- function(
  revenue_ts = NULL,
  method = "harmonic",
  h = 28,
  n_folds = 9,
  train_days = 1095
) {
  bt <- backtest_revenue_forecast(revenue_ts,
    h = h, n_folds = n_folds,
    methods = method, train_days = train_days,
    return_preds = TRUE
  )
  p <- bt$preds
  if (is.null(p) || nrow(p) == 0) {
    stop("Backtest lieferte keine Vorhersagen fuer die Kalibrierung.")
  }

  qd <- stats::quantile(p$revenue / p$mean, c(.025, .10, .90, .975), na.rm = TRUE)

  # Horizont-Summe je Fold: Ist-Summe / Prognose-Summe
  hs <- p |>
    dplyr::group_by(origin) |>
    dplyr::summarise(ratio = sum(revenue) / sum(mean), .groups = "drop")
  qh <- stats::quantile(hs$ratio, c(.025, .10, .90, .975), na.rm = TRUE)

  nm <- c("p025", "p10", "p90", "p975")
  list(
    daily = stats::setNames(as.numeric(qd), nm),
    horizon = stats::setNames(as.numeric(qh), nm),
    n_folds = length(unique(p$origin))
  )
}


#' Prognose zu Wochen-/Monats-/Horizont-Aggregaten verdichten
#'
#' @title summarise_revenue_forecast
#' @description Aggregiert die Tagesprognose zu planungstauglichen Blöcken. Die
#'   Punktsumme ist exakt; die Intervalle nutzen — wenn vorhanden — die
#'   Horizont-Conformal-Ratios (\code{$horizon} aus
#'   \code{calibrate_revenue_intervals()}), die die Fehler-Auslöschung über die
#'   Periode korrekt einfangen (viel enger als die Summe der Tagesbänder).
#' @param fc tibble aus \code{build_revenue_forecast()}.
#' @param by Aggregationsebene: \code{"week"}, \code{"month"} oder \code{"total"}.
#'   Default \code{"week"}.
#' @param conformal Optional \code{calibrate_revenue_intervals()}-Ergebnis für
#'   kalibrierte Aggregat-Intervalle. Default \code{NULL} (dann Summe der
#'   Tagesbänder als grobe Näherung).
#' @return tibble mit \code{periode, prognose, lo80, hi80} (und bei
#'   \code{by="total"} zusätzlich \code{lo95, hi95}).
#' @importFrom dplyr mutate group_by summarise
#' @importFrom tsibble yearweek yearmonth
#' @importFrom tibble tibble
#' @export
#' @examples
#' \dontrun{
#' fc <- build_revenue_forecast(h = 90)
#' cal <- calibrate_revenue_intervals(h = 90)
#' summarise_revenue_forecast(fc, by = "month", conformal = cal)
#' }
summarise_revenue_forecast <- function(fc, by = c("week", "month", "total"),
                                       conformal = NULL) {
  by <- match.arg(by)

  if (by == "total") {
    tot <- sum(fc$.mean)
    if (!is.null(conformal) && !is.null(conformal$horizon)) {
      q <- conformal$horizon
      return(tibble::tibble(
        periode = "Horizont gesamt", prognose = tot,
        lo80 = tot * q[["p10"]], hi80 = tot * q[["p90"]],
        lo95 = tot * q[["p025"]], hi95 = tot * q[["p975"]]
      ))
    }
    return(tibble::tibble(
      periode = "Horizont gesamt", prognose = tot,
      lo80 = sum(fc$lo80), hi80 = sum(fc$hi80),
      lo95 = sum(fc$lo95), hi95 = sum(fc$hi95)
    ))
  }

  key <- if (by == "week") tsibble::yearweek(fc$date) else tsibble::yearmonth(fc$date)
  agg <- tibble::tibble(
    periode = key, .mean = fc$.mean,
    lo80 = fc$lo80, hi80 = fc$hi80
  ) |>
    dplyr::group_by(periode) |>
    dplyr::summarise(
      prognose = sum(.mean),
      lo80 = sum(lo80), hi80 = sum(hi80), .groups = "drop"
    )

  # Kalibrierte Aggregat-Bänder: Horizont-Ratio auf die Blocksumme anwenden
  if (!is.null(conformal) && !is.null(conformal$horizon)) {
    q <- conformal$horizon
    agg$lo80 <- agg$prognose * q[["p10"]]
    agg$hi80 <- agg$prognose * q[["p90"]]
  }
  agg
}


# kleiner NULL-Coalesce-Helfer (falls nicht global vorhanden)
`%||%` <- function(a, b) if (is.null(a)) b else a
