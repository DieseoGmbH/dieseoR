# =============================================================================
# build_revenue_forecast_bundle.R  —  Nacht-Vorberechnung fürs Dashboard
#
# EINE Funktion, die alles rechnet, was der "Prognose & Planung"-Tab braucht,
# und ein schlankes RDS zurückgibt. Gehört in die Nacht-Pipeline
# (update_dashboard.R) — NICHT reaktiv im Dashboard (Regel: vorberechnen statt
# live rechnen). Das Dashboard liest das RDS nur noch und rendert.
#
# Enthält: Kurzfrist-Tagesprognose (brutto) + kalibrierte Bänder, Netto-Linie
# über die Retourenquote, Monats-/Rest-des-Jahres-Aggregate, Langfrist-Szenarien
# bis 2030 und ein Backtest-Gütepanel (Vertrauensbeleg).
# =============================================================================


#' Prognose-Bundle fürs Dashboard vorberechnen
#'
#' @title build_revenue_forecast_bundle
#' @description Orchestriert die komplette Umsatzprognose für den Dashboard-Tab
#'   "Prognose & Planung" und liefert ein schlankes, render-fertiges Listen-
#'   Objekt (zum \code{saveRDS} in die Serving-Daten). Brutto ist die
#'   Prognosebasis (sauberes Nachfragesignal); Netto wird über die reale
#'   Retourenquote abgeleitet und als zweite Linie/Kennzahl geführt. Alle
#'   schweren Berechnungen (ARIMA-Fit, Backtest-Kalibrierung) laufen hier einmal
#'   nächtlich.
#' @param duckdb_path Pfad zur Dashboard-DuckDB (Data Mart). Default Serving-Pfad.
#' @param returns_path Pfad zu \code{all_returns_cleaned.rds} (für die
#'   Retourenquote). \code{NULL} = Netto überspringen. Default Serving-Pfad.
#' @param h_daily Länge der operativen Tagesprognose im Chart (Tage). Default 90.
#' @param to_year Zieljahr der Langfrist-Szenarien. Default 2030.
#' @param n_folds Rolling-Origin-Folds für Backtest & Kalibrierung. Default 9.
#' @param net_window_days Fenster für die Retourenquote (Tage, jüngste). Default 180.
#' @param generated_at Zeitstempel für die Fußzeile (Skripte reichen \code{Sys.time()};
#'   als Parameter, weil Package-Funktionen reproduzierbar bleiben sollen).
#'   Default \code{Sys.time()}.
#' @return Benannte Liste: \code{daily} (Tagesprognose brutto+netto+Bänder),
#'   \code{history} (jüngste Ist-Reihe brutto+netto), \code{monthly},
#'   \code{rest_of_year}, \code{full_year}, \code{annual}, \code{scenarios},
#'   \code{backtest}, \code{calibration}, \code{meta}.
#' @importFrom dplyr filter mutate group_by summarise left_join arrange transmute n
#' @importFrom tsibble as_tibble yearmonth
#' @importFrom tibble tibble
#' @importFrom tidyr replace_na
#' @importFrom stats quantile setNames
#' @export
#' @examples
#' \dontrun{
#' bundle <- build_revenue_forecast_bundle()
#' saveRDS(bundle, "~/git/dashboard/data/revenue_forecast.rds")
#' }
build_revenue_forecast_bundle <- function(
  duckdb_path = "~/git/dashboard/data/shopify.duckdb",
  returns_path = "~/git/dashboard/data/all_returns_cleaned.rds",
  h_daily = 90,
  to_year = 2030,
  n_folds = 9,
  net_window_days = 180,
  generated_at = Sys.time()
) {
  # --- 1. Gross-Reihe + Horizont bis Jahresende --------------------------------
  rev_ts <- get_daily_revenue(duckdb_path = duckdb_path, drop_last_day = TRUE)
  last_day <- max(rev_ts$date)
  dec31 <- as.Date(sprintf("%d-12-31", as.integer(format(last_day, "%Y"))))
  h_year <- max(as.integer(dec31 - last_day), h_daily) # bis Jahresende, min. Chart-Horizont

  # --- 2. Backtest EINMAL (Gütepanel) + Kalibrierung ---------------------------
  bt <- backtest_revenue_forecast(rev_ts,
    h = 28, n_folds = n_folds,
    methods = c("harmonic", "ets", "snaive")
  )
  backtest <- bt |>
    dplyr::group_by(.model) |>
    dplyr::summarise(
      folds = dplyr::n(),
      MAE = mean(mae), MAPE = mean(mape), RMSE = mean(rmse),
      sumAPE = mean(sumape), cov80 = mean(cov80), cov95 = mean(cov95),
      .groups = "drop"
    ) |>
    dplyr::arrange(MAE)

  # Kalibrierung bei h=28: viele Folds -> STABILE Horizont-Ratios (~1 Monat).
  # (Lange Horizonte hätten zu wenige Folds -> verrauschte Quantile.)
  cal <- calibrate_revenue_intervals(rev_ts,
    method = "harmonic",
    h = 28, n_folds = n_folds
  )

  # --- 3. Produktionsprognose bis Jahresende ----------------------------------
  # Tageschart nutzt die ANALYTISCHEN Modell-Bänder (bereits kalibriert,
  # cov80~82%, und weiten sich mit dem Horizont natürlich). Aggregate nutzen
  # die stabilen h=28-Conformal-Ratios (siehe Monats-/Rest-Berechnung unten).
  fc_full <- build_revenue_forecast(rev_ts,
    h = h_year, method = "harmonic",
    conformal = NULL
  )

  # --- 4. Netto über Retourenquote --------------------------------------------
  net_rate <- NA_real_
  refunds_daily <- NULL
  if (!is.null(returns_path) && file.exists(path.expand(returns_path))) {
    rt <- readRDS(path.expand(returns_path))
    rf <- rt |>
      dplyr::filter(status == "refunded", type %in% c("return", "mix")) |>
      dplyr::mutate(date = as.Date(order_completed_date)) |>
      dplyr::filter(!is.na(date))
    refunds_daily <- rf |>
      dplyr::group_by(date) |>
      dplyr::summarise(refunds = sum(refund_amount, na.rm = TRUE), .groups = "drop")
    # Netto-Quote aus jüngstem Fenster (cash-basis): 1 - refunds/gross
    g_win <- sum(rev_ts$revenue[rev_ts$date > last_day - net_window_days])
    r_win <- sum(refunds_daily$refunds[refunds_daily$date > last_day - net_window_days])
    net_rate <- if (g_win > 0) 1 - r_win / g_win else NA_real_
  }
  nr <- if (is.na(net_rate)) 1 else net_rate # Fallback: netto=brutto

  # --- 5. Tages-Chart-Daten (erste h_daily Tage) ------------------------------
  daily <- fc_full |>
    dplyr::filter(date <= last_day + h_daily) |>
    dplyr::transmute(date,
      gross = .mean, lo80, hi80, lo95, hi95,
      net = .mean * nr
    )

  # --- 6. Historie (jüngste ~150 Tage, brutto + netto) ------------------------
  hist_tail <- rev_ts |>
    tsibble::as_tibble() |>
    dplyr::filter(date > last_day - 150) |>
    dplyr::mutate(date = as.Date(date))
  if (!is.null(refunds_daily)) {
    hist_tail <- hist_tail |>
      dplyr::left_join(refunds_daily, by = "date") |>
      dplyr::mutate(
        refunds = tidyr::replace_na(refunds, 0),
        net = pmax(revenue - refunds, 0)
      )
  } else {
    hist_tail$net <- hist_tail$revenue
  }
  history <- dplyr::transmute(hist_tail, date, gross = revenue, net)

  # --- 7. Monats-Aggregate (nur Prognosemonate bis Jahresende) ----------------
  # Direkt über Monatsanfang (Date) + deutsches Label aggregieren -> das Dashboard
  # muss keine yearmonth-Klasse formatieren (round-trip-sicher).
  de_month <- c(
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember"
  )
  qh <- cal$horizon
  monthly <- dplyr::filter(fc_full, date <= dec31) |>
    dplyr::mutate(mstart = as.Date(paste0(format(date, "%Y-%m"), "-01"))) |>
    dplyr::group_by(mstart) |>
    dplyr::summarise(prognose = sum(.mean), .groups = "drop") |>
    dplyr::arrange(mstart) |>
    dplyr::mutate(
      label = paste(de_month[as.integer(format(mstart, "%m"))], format(mstart, "%Y")),
      lo80  = prognose * qh[["p10"]],
      hi80  = prognose * qh[["p90"]],
      net   = prognose * nr
    )

  # --- 8. Rest des Jahres + Gesamtjahr ----------------------------------------
  # Rest-of-Year-Band = Summe der Monatsbänder: der Drift-/Wachstumsfehler ist
  # über Monate KORRELIERT, daher ist Aufsummieren (nicht Quadratur) die
  # ehrliche, leicht konservative Wahl.
  roy_gross <- sum(monthly$prognose)
  roy_lo80 <- sum(monthly$lo80)
  roy_hi80 <- sum(monthly$hi80)
  ytd <- sum(rev_ts$revenue[format(rev_ts$date, "%Y") == format(last_day, "%Y")])
  rest_of_year <- tibble::tibble(
    gross = roy_gross, lo80 = roy_lo80, hi80 = roy_hi80,
    net = roy_gross * nr
  )

  # --- 9. Langfrist-Szenarien + Momentum-Vergleich ----------------------------
  annual <- get_annual_revenue(rev_ts, annualize_current = TRUE)
  # Anker explizit übergeben -> project_revenue_scenarios lädt NICHT erneut die
  # (default-)DuckDB, sondern nutzt die bereits geladene Reihe.
  full_actual <- annual[annual$status == "actual", , drop = FALSE]
  anchor <- full_actual[which.max(full_actual$year), ]
  scenarios <- project_revenue_scenarios(
    base_value = anchor$revenue,
    base_year = anchor$year,
    to_year = to_year
  )

  # ZWEI ehrliche Gesamtjahres-Anker (divergieren am Q4-Peak!):
  #  * model    = Bottom-up-Prognose (konservativ am Peak: bildet YoY-Momentum
  #               nicht voll ab -> Nov ~ flach ggü. Vorjahr).
  #  * momentum = saisonale Hochrechnung (YoY-Momentum hält an) aus get_annual_revenue.
  cur_yr <- as.integer(format(last_day, "%Y"))
  momentum_gross <- {
    a <- annual[annual$status == "annualized" & annual$year == cur_yr, ]
    if (nrow(a) == 1) a$revenue else NA_real_
  }
  full_year <- tibble::tibble(
    year = cur_yr,
    ytd_actual_gross = ytd,
    forecast_rest_gross = roy_gross,
    model_total_gross = ytd + roy_gross, # Bottom-up-Modell
    model_total_lo80 = ytd + roy_lo80,
    model_total_hi80 = ytd + roy_hi80,
    momentum_total_gross = momentum_gross, # YoY-Momentum-Hochrechnung
    model_total_net = (ytd + roy_gross) * nr,
    momentum_total_net = momentum_gross * nr
  )

  # --- 10. Meta ---------------------------------------------------------------
  meta <- list(
    generated_at = generated_at,
    data_from = min(rev_ts$date),
    data_to = last_day,
    n_days = nrow(rev_ts),
    model = unique(fc_full$.model),
    interval_daily = "model (analytisch, kalibriert)",
    interval_aggr = "conformal (Backtest h=28)",
    net_rate = nr,
    net_available = !is.na(net_rate),
    h_daily = h_daily,
    h_year = h_year,
    revenue_basis = "brutto = SUM(item_gross_revenue), nicht storniert, inkl. USt.; netto = brutto x (1 - Retourenquote)",
    caveat_q4 = paste(
      "Kurzfrist (Tage-Wochen) ist validiert (cov80~82%).",
      "Gesamtjahr/Q4: das Modell ist am Black-Friday-Peak konservativ",
      "(bildet das +100% YoY-Momentum nicht voll ab). Deshalb zwei Anker:",
      "model (bottom-up) vs. momentum (YoY-Hochrechnung)."
    )
  )

  list(
    daily = daily, history = history, monthly = monthly,
    rest_of_year = rest_of_year, full_year = full_year,
    annual = annual, scenarios = scenarios,
    backtest = backtest, calibration = cal, meta = meta
  )
}
