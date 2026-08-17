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
#
# AUFBAU (seit 08/2026, Länder-Split):
#   .forecast_slice()               rechnet EINE Reihe komplett durch
#   build_revenue_forecast_bundle() ruft das für Gesamt + je Land auf
# Die Wurzel des Bundles bleibt unverändert die GESAMT-Sicht (rückwärts-
# kompatibel zu älteren app.R-Ständen); der Länder-Split hängt zusätzlich unter
# $by_country. Alle Slices haben exakt dieselbe Struktur -> das Dashboard
# tauscht beim Umschalten nur die Quell-Liste aus, keine Sonderlogik.
# =============================================================================


# --- intern: Ländercode -> deutsches Label (Anzeige im Dashboard) ------------
.country_labels_de <- c(
  DE = "Deutschland", AT = "Österreich", CH = "Schweiz", FR = "Frankreich",
  ES = "Spanien", IT = "Italien", NL = "Niederlande", BE = "Belgien",
  GB = "Vereinigtes Königreich", IE = "Irland", DK = "Dänemark",
  SE = "Schweden", NO = "Norwegen", FI = "Finnland", PL = "Polen",
  CZ = "Tschechien", SK = "Slowakei", SI = "Slowenien", HU = "Ungarn",
  HR = "Kroatien", RO = "Rumänien", BG = "Bulgarien", GR = "Griechenland",
  PT = "Portugal", LU = "Luxemburg", CY = "Zypern", MT = "Malta",
  EE = "Estland", LV = "Lettland", LT = "Litauen", US = "USA",
  CA = "Kanada", AE = "VAE", TH = "Thailand", ID = "Indonesien",
  AU = "Australien", TR = "Türkei"
)

# --- intern: Retourenportal-Landname (klein) -> ISO-2 ------------------------
# Bewusst EXPLIZIT: ein unbekannter Name wird NA und landet damit im
# "Übrige Länder"-Bucket. Ein Fuzzy-Match würde im Zweifel dem falschen Land
# Erstattungen zuordnen und dort die Netto-Quote verfälschen.
.returns_country_codes <- c(
  germany = "DE", austria = "AT", switzerland = "CH", france = "FR",
  spain = "ES", italy = "IT", netherlands = "NL", belgium = "BE",
  `united kingdom` = "GB", ireland = "IE", denmark = "DK", sweden = "SE",
  norway = "NO", finland = "FI", poland = "PL", czechia = "CZ",
  `czech republic` = "CZ", slovakia = "SK", slovenia = "SI", hungary = "HU",
  croatia = "HR", romania = "RO", bulgaria = "BG", greece = "GR",
  portugal = "PT", luxembourg = "LU", cyprus = "CY", malta = "MT",
  estonia = "EE", latvia = "LV", lithuania = "LT",
  `united states` = "US", canada = "CA",
  `united arab emirates` = "AE", thailand = "TH", indonesia = "ID",
  australia = "AU", turkey = "TR"
)


# --- intern: Erstattungen je Tag UND Land aus dem Retourenportal -------------
# Rückgabe: tibble(date, country_code, refunds) oder NULL.
.load_refunds_by_country <- function(returns_path) {
  if (is.null(returns_path) || !file.exists(path.expand(returns_path))) {
    return(NULL)
  }
  rt <- readRDS(path.expand(returns_path))
  rf <- rt |>
    dplyr::filter(status == "refunded", type %in% c("return", "mix")) |>
    dplyr::mutate(
      date = as.Date(order_completed_date),
      .cname = tolower(trimws(country)),
      country_code = unname(.returns_country_codes[.cname])
    ) |>
    dplyr::filter(!is.na(date))

  # Drift-Wächter: taucht im Portal ein neuer Landname auf, den die Map nicht
  # kennt, wandern dessen Erstattungen still in den Restbucket und verzerren
  # dort die Netto-Quote. Lieber laut sein als leise falsch.
  unmapped <- rf |>
    dplyr::filter(is.na(country_code), !is.na(.cname), nzchar(.cname)) |>
    dplyr::group_by(.cname) |>
    dplyr::summarise(refunds = sum(refund_amount, na.rm = TRUE), .groups = "drop")
  if (nrow(unmapped) > 0) {
    tot <- sum(rf$refund_amount, na.rm = TRUE)
    big <- unmapped[unmapped$refunds > 0.005 * tot, ]
    if (nrow(big) > 0) {
      warning(
        "Retouren-Laender ohne Code-Mapping (landen im Restbucket): ",
        paste(sprintf("%s (%.1f %%)", big$.cname, 100 * big$refunds / tot),
          collapse = ", "
        ), " -- .returns_country_codes ergaenzen."
      )
    }
  }

  rf |>
    dplyr::group_by(date, country_code) |>
    dplyr::summarise(refunds = sum(refund_amount, na.rm = TRUE), .groups = "drop")
}


# --- intern: Erstattungsreihe für einen Slice zusammenziehen -----------------
.refunds_for_slice <- function(refunds_by_country, codes = NULL, exclude = NULL) {
  if (is.null(refunds_by_country)) {
    return(NULL)
  }
  x <- refunds_by_country
  if (!is.null(codes)) {
    x <- dplyr::filter(x, !is.na(country_code), country_code %in% codes)
  }
  if (!is.null(exclude)) {
    # Ohne Code = keinem modellierten Land zuzuordnen -> gehört in den Restbucket
    x <- dplyr::filter(x, is.na(country_code) | !(country_code %in% exclude))
  }
  if (nrow(x) == 0) {
    return(NULL)
  }
  x |>
    dplyr::group_by(date) |>
    dplyr::summarise(refunds = sum(refunds, na.rm = TRUE), .groups = "drop")
}


# --- intern: Tragfähigkeit einer Reihe messen -------------------------------
# Dieselben zwei Kennzahlen wie in get_revenue_country_shares(), aber direkt an
# der fertigen Reihe — für Slices (Restbucket), die kein einzelnes Land sind.
# Reihen aus get_daily_revenue(gap_fill = "zero") haben auf umsatzlosen Tagen
# exakt 1 (Log-Boden), deshalb ist `> 1` die Prüfung auf "Tag mit Umsatz".
.slice_quality <- function(ts) {
  list(
    calendar_days = as.integer(max(ts$date) - min(ts$date)) + 1L,
    fill_rate = mean(ts$revenue > 1)
  )
}


# --- intern: EINE Umsatzreihe komplett durchrechnen --------------------------
# Der frühere Rumpf von build_revenue_forecast_bundle(). Bekommt die fertige
# Reihe und die passende Erstattungsreihe herein, macht selbst keinen I/O ->
# Gesamt und jedes Land laufen garantiert durch denselben Code.
.forecast_slice <- function(rev_ts,
                            refunds_daily = NULL,
                            h_daily = 90,
                            to_year = 2030,
                            n_folds = 9,
                            net_window_days = 180,
                            method = "harmonic",
                            methods = c("harmonic", "ets", "snaive")) {
  last_day <- max(rev_ts$date)
  dec31 <- as.Date(sprintf("%d-12-31", as.integer(format(last_day, "%Y"))))
  h_year <- max(as.integer(dec31 - last_day), h_daily) # bis Jahresende, min. Chart-Horizont

  # --- Backtest (Gütepanel) + Modellwahl --------------------------------------
  bt <- backtest_revenue_forecast(rev_ts, h = 28, n_folds = n_folds, methods = methods)
  if (nrow(bt) == 0) {
    stop("Backtest lieferte keine Folds — Reihe zu kurz fuer diesen Slice.")
  }
  backtest <- bt |>
    dplyr::group_by(.model) |>
    dplyr::summarise(
      folds = dplyr::n(),
      MAE = mean(mae), MAPE = mean(mape), RMSE = mean(rmse),
      sumAPE = mean(sumape), cov80 = mean(cov80), cov95 = mean(cov95),
      .groups = "drop"
    ) |>
    dplyr::arrange(MAE)

  # WARUM KEINE automatische Modellwahl aus dieser Tabelle (getestet 08/2026):
  # Der Backtest läuft auf h=28, die Produktionsprognose auf h~140. ETS gewinnt
  # den h=28-Vergleich regelmäßig (DE 36,4 % vs. 37,4 % sumAPE, AT 35,1 % vs.
  # 46,0 %), explodiert aber auf dem langen Horizont: AT käme damit auf 48 M€
  # "Rest des Jahres" bei 6,2 M€ YTD, DE auf 120 M€ bei 86 M€ YTD. Das ist
  # genau das dokumentierte ETS-Versagen im Winter (multiplikative Saison
  # kumuliert). Eine Auswahl auf 28 Tagen darf ein Modell für 140 Tage nicht
  # bestimmen -> harmonic bleibt fest verdrahtet, ets/snaive laufen nur als
  # Referenzzeilen im Gütepanel mit.
  backtest$chosen <- backtest$.model == method

  # Kalibrierung bei h=28: viele Folds -> STABILE Horizont-Ratios (~1 Monat).
  # (Lange Horizonte hätten zu wenige Folds -> verrauschte Quantile.)
  cal <- calibrate_revenue_intervals(rev_ts, method = method, h = 28, n_folds = n_folds)

  # --- Produktionsprognose bis Jahresende -------------------------------------
  # Tageschart nutzt die ANALYTISCHEN Modell-Bänder (bereits kalibriert,
  # cov80~82%, und weiten sich mit dem Horizont natürlich). Aggregate nutzen
  # die stabilen h=28-Conformal-Ratios (siehe Monats-/Rest-Berechnung unten).
  fc_full <- build_revenue_forecast(rev_ts, h = h_year, method = method, conformal = NULL)

  # --- Netto über Retourenquote -----------------------------------------------
  net_rate <- NA_real_
  if (!is.null(refunds_daily) && nrow(refunds_daily) > 0) {
    g_win <- sum(rev_ts$revenue[rev_ts$date > last_day - net_window_days])
    r_win <- sum(refunds_daily$refunds[refunds_daily$date > last_day - net_window_days])
    net_rate <- if (g_win > 0) 1 - r_win / g_win else NA_real_
    # Kappen: bei kleinen Slices kann ein Zuordnungsfehler die Quote sonst
    # aus dem Wertebereich tragen (negativer "Umsatz" im Chart).
    if (!is.na(net_rate)) net_rate <- max(0, min(1, net_rate))
  }
  nr <- if (is.na(net_rate)) 1 else net_rate # Fallback: netto=brutto

  # --- Tages-Chart-Daten (erste h_daily Tage) ---------------------------------
  daily <- fc_full |>
    dplyr::filter(date <= last_day + h_daily) |>
    dplyr::transmute(date,
      gross = .mean, lo80, hi80, lo95, hi95,
      net = .mean * nr
    )

  # --- Historie (jüngste ~150 Tage, brutto + netto) ---------------------------
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

  # --- Monats-Aggregate (nur Prognosemonate bis Jahresende) -------------------
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

  # --- Rest des Jahres + Gesamtjahr -------------------------------------------
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

  # --- Langfrist-Szenarien + Momentum-Vergleich -------------------------------
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

  # Güte des GEWÄHLTEN Modells direkt mitgeben -> das Dashboard muss die
  # Backtest-Tabelle nicht filtern (und kann sich nicht auf "harmonic" verlassen).
  chosen_row <- backtest[backtest$chosen, ]
  meta <- list(
    data_from = min(rev_ts$date),
    data_to = last_day,
    n_days = nrow(rev_ts),
    model = unique(fc_full$.model),
    method = method,
    sum_ape = if (nrow(chosen_row) == 1) chosen_row$sumAPE else NA_real_,
    cov80 = if (nrow(chosen_row) == 1) chosen_row$cov80 else NA_real_,
    interval_daily = "model (analytisch, kalibriert)",
    interval_aggr = "conformal (Backtest h=28)",
    net_rate = nr,
    net_available = !is.na(net_rate),
    h_daily = h_daily,
    h_year = h_year
  )

  list(
    daily = daily, history = history, monthly = monthly,
    rest_of_year = rest_of_year, full_year = full_year,
    annual = annual, scenarios = scenarios,
    backtest = backtest, calibration = cal, meta = meta
  )
}


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
#'
#'   Zusätzlich wird die Prognose nach \strong{Lieferland} aufgesplittet
#'   (\code{by_country}). Ein eigenes Modell bekommt nur, wer es datenseitig
#'   trägt: genug Umsatzanteil, >= 2 Jahressaison-Zyklen Historie und eine
#'   ausreichend dichte Tagesreihe (Kriterien via \code{country_min_*}). Alle
#'   übrigen Länder — inklusive Bestellungen ohne Ländercode — werden zu einem
#'   Sammel-Slice gebündelt, damit die Slices in Summe wieder die Gesamtreihe
#'   ergeben. Jeder Slice hat exakt dieselbe Struktur wie die Wurzel.
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
#' @param by_country Länder-Split mitrechnen? Default \code{TRUE}.
#' @param country_min_share Mindest-Umsatzanteil (letzte 365 Tage) für ein
#'   eigenes Modell. Default \code{0.02} (2 \%).
#' @param country_min_days Mindest-Historie in Kalendertagen. Default \code{730}
#'   (das Harmonic-ARIMA braucht >= 2 Jahressaison-Zyklen).
#' @param country_min_fill Mindestanteil Tage mit Umsatz an der Historie.
#'   Default \code{0.90}. Schützt vor log-Modellen auf löchrigen Reihen.
#' @param country_other_key,country_other_label Schlüssel und Anzeigename des
#'   Sammel-Slices. Defaults \code{"OTHER"} / \code{"Übrige Länder"}.
#' @param method Produktivmodell für alle Slices. Default \code{"harmonic"} —
#'   per Bake-off auf der Gesamtreihe entschieden und bewusst fest verdrahtet.
#'   Eine Auswahl je Slice aus dem h=28-Backtest wäre ein Fehlschluss: ETS
#'   gewinnt dort regelmäßig, versagt aber auf dem Produktionshorizont h~140
#'   (siehe Kommentar in \code{.forecast_slice}).
#' @param country_methods Im Länder-Backtest zusätzlich mitgerechnete Modelle.
#'   Default alle drei — sie erscheinen als Referenzzeilen im Gütepanel, damit
#'   die Modellwahl je Land nachprüfbar bleibt.
#' @return Benannte Liste. Wurzel = \strong{Gesamt}-Sicht (unverändert
#'   rückwärtskompatibel): \code{daily}, \code{history}, \code{monthly},
#'   \code{rest_of_year}, \code{full_year}, \code{annual}, \code{scenarios},
#'   \code{backtest}, \code{calibration}, \code{meta}. Neu:
#'   \code{by_country} (benannte Liste gleich strukturierter Slices, Schlüssel =
#'   ISO-2-Code bzw. \code{country_other_key}) und \code{countries}
#'   (Auswahl-/Diagnosetabelle mit \code{key, label, role, share, modelled,
#'   reason}).
#' @importFrom dplyr filter mutate group_by summarise left_join arrange transmute n bind_rows
#' @importFrom tsibble as_tibble yearmonth
#' @importFrom tibble tibble
#' @importFrom tidyr replace_na
#' @importFrom stats quantile setNames
#' @export
#' @examples
#' \dontrun{
#' bundle <- build_revenue_forecast_bundle()
#' saveRDS(bundle, "~/git/dashboard/data/revenue_forecast.rds")
#' names(bundle$by_country)
#' }
build_revenue_forecast_bundle <- function(
  duckdb_path = "~/git/dashboard/data/shopify.duckdb",
  returns_path = "~/git/dashboard/data/all_returns_cleaned.rds",
  h_daily = 90,
  to_year = 2030,
  n_folds = 9,
  net_window_days = 180,
  generated_at = Sys.time(),
  by_country = TRUE,
  country_min_share = 0.02,
  country_min_days = 730,
  country_min_fill = 0.90,
  country_other_key = "OTHER",
  country_other_label = "Übrige Länder",
  method = "harmonic",
  country_methods = c("harmonic", "ets", "snaive")
) {
  # --- 1. Erstattungen einmal laden (Tag x Land) -------------------------------
  refunds_by_country <- .load_refunds_by_country(returns_path)

  # --- 2. Gesamt-Slice (= Wurzel des Bundles) ----------------------------------
  rev_ts <- get_daily_revenue(duckdb_path = duckdb_path, drop_last_day = TRUE)
  out <- .forecast_slice(
    rev_ts,
    refunds_daily = .refunds_for_slice(refunds_by_country),
    h_daily = h_daily, to_year = to_year, n_folds = n_folds,
    net_window_days = net_window_days,
    method = method, methods = c("harmonic", "ets", "snaive")
  )

  # --- 3. Länder-Slices --------------------------------------------------------
  countries_tbl <- NULL
  slices <- list()
  if (isTRUE(by_country)) {
    shares <- get_revenue_country_shares(duckdb_path = duckdb_path)

    # Kriterien einzeln prüfen, damit die Ablehnung im Dashboard begründbar ist.
    # Die Texte gehen 1:1 in die UI -> deutsches Dezimalkomma.
    de_num <- function(x, digits = 1) sub("\\.", ",", sprintf(paste0("%.", digits, "f"), x))
    fails <- function(i) {
      r <- character(0)
      if (is.na(shares$country[i])) {
        return("kein Lieferland hinterlegt")
      }
      if (shares$share[i] < country_min_share) {
        r <- c(r, sprintf(
          "Umsatzanteil %s %% (< %s %%)",
          de_num(100 * shares$share[i]), de_num(100 * country_min_share, 0)
        ))
      }
      if (shares$calendar_days[i] < country_min_days) {
        r <- c(r, sprintf(
          "Historie %d Tage (< %d)",
          as.integer(shares$calendar_days[i]), as.integer(country_min_days)
        ))
      }
      if (shares$fill_rate[i] < country_min_fill) {
        r <- c(r, sprintf(
          "Tagesreihe nur %s %% gefüllt (< %s %%)",
          de_num(100 * shares$fill_rate[i], 0), de_num(100 * country_min_fill, 0)
        ))
      }
      paste(r, collapse = "; ")
    }
    reasons <- vapply(seq_len(nrow(shares)), fails, character(1))
    shares$modelled <- !nzchar(reasons)
    shares$reason <- ifelse(shares$modelled, NA_character_, reasons)

    codes <- shares$country[shares$modelled]

    # Je Land ein eigener tryCatch: ein kippendes Land darf die Nacht-Pipeline
    # (und damit auch die Gesamt-Prognose) nicht mitreissen.
    for (cd in codes) {
      slices[[cd]] <- tryCatch(
        {
          ts_c <- get_daily_revenue(
            duckdb_path = duckdb_path, drop_last_day = TRUE,
            countries = cd, gap_fill = "zero"
          )
          .forecast_slice(ts_c,
            refunds_daily = .refunds_for_slice(refunds_by_country, codes = cd),
            h_daily = h_daily, to_year = to_year, n_folds = n_folds,
            net_window_days = net_window_days,
            method = method, methods = country_methods
          )
        },
        error = function(e) {
          warning("Laender-Slice ", cd, " uebersprungen: ", conditionMessage(e))
          NULL
        }
      )
    }

    # --- Restbucket: alles ohne eigenes Modell (inkl. Orders ohne Ländercode) ---
    # Er muss DIESELBEN Qualitätshürden nehmen wie ein Einzelland. Sonst käme
    # durch die Hintertür genau die Reihe ins Dashboard, die man vorne gerade
    # als nicht modellierbar aussortiert hat: der Bucket aus FR/CH/ES/... ist
    # ein junger Markt (2025: 0,4 M€ -> 2026 YTD: 2,4 M€) mit über die Historie
    # nur ~60 % gefüllten Tagen. harmonic stülpt ihm die Q4-Saison der
    # Gesamtreihe über und prognostiziert 8,9 M€ Rest-des-Jahres bei 2,4 M€ YTD
    # (Backtest-sumAPE 331 % statt ~38 %).
    other_reason <- NA_character_
    ts_o <- tryCatch(
      get_daily_revenue(
        duckdb_path = duckdb_path, drop_last_day = TRUE,
        exclude_countries = codes, gap_fill = "zero"
      ),
      error = function(e) NULL
    )
    if (is.null(ts_o)) {
      other_reason <- "kein Umsatz außerhalb der modellierten Länder"
    } else {
      oq <- .slice_quality(ts_o)
      if (oq$calendar_days < country_min_days) {
        other_reason <- sprintf(
          "Historie %d Tage (< %d)", oq$calendar_days, as.integer(country_min_days)
        )
      } else if (oq$fill_rate < country_min_fill) {
        other_reason <- sprintf(
          "Tagesreihe nur %s %% gefüllt (< %s %%)",
          sub("\\.", ",", sprintf("%.0f", 100 * oq$fill_rate)),
          sub("\\.", ",", sprintf("%.0f", 100 * country_min_fill))
        )
      }
    }
    if (is.na(other_reason)) {
      slices[[country_other_key]] <- tryCatch(
        .forecast_slice(ts_o,
          refunds_daily = .refunds_for_slice(refunds_by_country, exclude = codes),
          h_daily = h_daily, to_year = to_year, n_folds = n_folds,
          net_window_days = net_window_days,
          method = method, methods = country_methods
        ),
        error = function(e) {
          warning("Restbucket-Slice uebersprungen: ", conditionMessage(e))
          NULL
        }
      )
    }
    slices <- slices[!vapply(slices, is.null, logical(1))]

    # --- 4. Auswahl-/Diagnosetabelle fürs Dashboard ----------------------------
    lbl <- function(cc) {
      unname(ifelse(cc %in% names(.country_labels_de), .country_labels_de[cc], cc))
    }
    ok <- shares[shares$modelled, ]
    ok <- ok[order(-ok$revenue_window), ]
    rest <- shares[!shares$modelled, ]

    countries_tbl <- dplyr::bind_rows(
      tibble::tibble(
        key = "TOTAL", label = "Gesamt", role = "total",
        share = 1, revenue_window = sum(shares$revenue_window),
        calendar_days = NA_integer_, fill_rate = NA_real_,
        modelled = TRUE, reason = NA_character_
      ),
      tibble::tibble(
        key = ok$country, label = lbl(ok$country), role = "country",
        share = ok$share, revenue_window = ok$revenue_window,
        calendar_days = as.integer(ok$calendar_days), fill_rate = ok$fill_rate,
        modelled = TRUE, reason = NA_character_
      ),
      tibble::tibble(
        key = country_other_key, label = country_other_label, role = "other",
        share = sum(rest$revenue_window) / sum(shares$revenue_window),
        revenue_window = sum(rest$revenue_window),
        calendar_days = NA_integer_, fill_rate = NA_real_,
        modelled = is.na(other_reason), reason = other_reason
      ),
      tibble::tibble(
        key = ifelse(is.na(rest$country), "(ohne Land)", rest$country),
        label = ifelse(is.na(rest$country), "(ohne Land)", lbl(rest$country)),
        role = "excluded",
        share = rest$share, revenue_window = rest$revenue_window,
        calendar_days = as.integer(rest$calendar_days), fill_rate = rest$fill_rate,
        modelled = FALSE, reason = rest$reason
      )
    )
    # Nur Slices anbieten, die auch wirklich gerechnet wurden
    countries_tbl$modelled <- countries_tbl$modelled &
      (countries_tbl$key == "TOTAL" | countries_tbl$key %in% names(slices))
  }

  # --- 5. Globale Meta-Angaben (gelten für alle Slices) ------------------------
  out$meta <- c(out$meta, list(
    generated_at = generated_at,
    revenue_basis = "brutto = SUM(item_gross_revenue), nicht storniert, inkl. USt.; netto = brutto x (1 - Retourenquote)",
    country_basis = "Lieferland (shipping_address_country_code); Netto je Land ueber die Retouren desselben Landes",
    country_scenarios = "Langfrist-Szenarien nutzen je Land dieselben Wachstumsannahmen, angewendet auf den jeweiligen Laender-Anker — Szenario-Planung, keine laenderspezifische Marktprognose",
    caveat_q4 = paste(
      "Kurzfrist (Tage-Wochen) ist validiert (cov80~82%).",
      "Gesamtjahr/Q4: das Modell ist am Black-Friday-Peak konservativ",
      "(bildet das +100% YoY-Momentum nicht voll ab). Deshalb zwei Anker:",
      "model (bottom-up) vs. momentum (YoY-Hochrechnung)."
    )
  ))

  out$by_country <- slices
  out$countries <- countries_tbl
  out
}
