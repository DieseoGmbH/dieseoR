# ==========================================================================
# GetKlar (Klar) Public Reporting API
# --------------------------------------------------------------------------
# Verifiziert am 07.08.2026 gegen die OpenAPI-Spec
# (https://api.getklar.com/public/docs/openapi.json) und Live-Aufrufe.
#
# ACHTUNG — der Help-Center-Artikel "Attribution API Documentation" beschreibt
# einen ueberholten Stand. Real gilt:
#   * Auth:  Header `X-API-Key: klar_pk_<64 hex>` — KEIN Token-Exchange,
#            kein `/public/auth/token`, kein Bearer.
#   * Pfade: `/v1/public/...` (versioniert), nicht `/public/...`
#   * endDate-Semantik ist NICHT STABIL: laut Spec exklusiv, am 07.08.2026 auch
#     so beobachtet — am 17.08.2026 lieferte derselbe Request den Endtag mit
#     (inklusiv). Die Abruffunktionen schneiden das Ergebnis deshalb selbst auf
#     den angeforderten Zeitraum zu und sind damit gegen beide Varianten robust.
#   * Zwei-Schritt-Flow: Report anstossen -> `dataUrl` -> Seiten via `nextPage`
#   * Kein Rate-Limit (60+ Seiten in Folge ohne 429)
#   * ZWEI Grenzen, beide endpunktabhaengig und beide verifiziert:
#       - `attribution`: max. 31 Tage je Request. 31 Tage -> 200, 32 Tage -> 400.
#         `attribution-detail`, `marketing` und `revenue-and-profit` haben das
#         NICHT (revenue laeuft mit 400 Tagen). Das 31-Tage-Limit aus dem
#         Help-Center gilt also nur fuer diesen einen Endpunkt.
#       - Cursor endet nach 100 Seiten a 1.000 Zeilen. Seite 100 kommt ohne
#         `nextPage`, Seite 101 liefert HTTP 200 mit leerem Ergebnis — obwohl
#         weitere Daten existieren. Verifiziert auf `attribution` (zwei Views)
#         und `attribution-detail`. Fuer `revenue-and-profit` und `marketing`
#         UNGEPRUEFT: unsere Daten erreichen dort keine 100.000 Zeilen. Die
#         Warnung in get_getklar_report() greift dort trotzdem.
#   * Fehlermeldungen sind nicht diagnostisch: Zeitraum zu gross UND das nicht
#     verfuegbare Modell `any_click` liefern beide denselben Text
#     ("Bad Request - Unknown error: Request failed with status code 418").
# ==========================================================================


#' @title Get the GetKlar API Key from the Environment
#' @description Interner Helper. Liest `GETKLAR_API_KEY`, faellt auf den
#'   historischen Namen `GETKLAR_REFRESH_TOKEN` zurueck.
#' @return character. Der API-Key.
#' @keywords internal
.getklar_key <- function() {
  key <- Sys.getenv("GETKLAR_API_KEY")
  if (!nzchar(key)) key <- Sys.getenv("GETKLAR_REFRESH_TOKEN")
  if (!nzchar(key)) {
    stop(
      "API-Key fehlt! Bitte in der .Renviron als GETKLAR_API_KEY setzen ",
      "(Format: klar_pk_<64 Hex-Zeichen>)."
    )
  }
  key
}


#' @title List All GetKlar Shops (Views)
#' @description Liefert alle Views des Accounts. Die `shopId` ist ein signiertes
#'   Token und wird von allen Report-Endpunkten als Pflichtparameter erwartet.
#'
#'   Bei Pammys existieren 10 Views: `Finance View` (Umsatz & Profit — die
#'   Referenz fuer Umsatzzahlen), `Marketing View` (global, Basis fuer
#'   Attribution) sowie acht Laender-Views (`Marketing View - DACH/FR/ES/DK/
#'   UK/NL/SE/BE`).
#' @param api_key character. Default via `.Renviron`.
#' @return Ein Tibble mit `shopId` und `name`.
#' @importFrom httr RETRY add_headers status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr as_tibble
#' @export
#' @examples \dontrun{
#' get_getklar_shops()
#' }
get_getklar_shops <- function(api_key = .getklar_key()) {
  res <- httr::RETRY(
    verb = "GET",
    url = "https://api.getklar.com/v1/public/shops",
    httr::add_headers(`X-API-Key` = api_key),
    times = 3, pause_base = 2, pause_cap = 10
  )

  if (httr::status_code(res) != 200) {
    stop(
      "GetKlar /shops fehlgeschlagen! Status: ", httr::status_code(res),
      " | ", httr::content(res, as = "text", encoding = "UTF-8")
    )
  }

  jsonlite::fromJSON(httr::content(res, as = "text", encoding = "UTF-8")) |>
    dplyr::as_tibble()
}


#' @title Resolve a GetKlar Shop Name to its Signed shopId
#' @description Interner Helper. Laesst eine bereits aufgeloeste `shopId`
#'   unveraendert durch und schlaegt Klarnamen ueber `get_getklar_shops()` nach.
#' @param shop character. View-Name (z. B. "Finance View") oder eine `shopId`.
#' @param api_key character.
#' @return character. Die signierte `shopId`.
#' @keywords internal
.getklar_shop_id <- function(shop, api_key) {
  # Signierte IDs haben das Format $SKID:...:$ — dann kein Lookup noetig
  if (grepl("^\\$SKID:", shop)) {
    return(shop)
  }

  shops <- get_getklar_shops(api_key = api_key)
  hit <- shops$shopId[shops$name == shop]

  if (length(hit) == 0) {
    stop(
      "Unbekannter Shop/View: '", shop, "'. Verfuegbar: ",
      paste(shops$name, collapse = " | ")
    )
  }
  hit[1]
}


#' @title Fetch a Paginated GetKlar Report
#' @description Generischer Zwei-Schritt-Abruf fuer alle Report-Endpunkte der
#'   Klar Public API: stoesst den Report an, folgt der `dataUrl` und
#'   anschliessend jeder `nextPage`, bis keine mehr kommt (1.000 Zeilen/Seite).
#' @param report character. Endpunkt ohne Praefix: `revenue-and-profit`,
#'   `attribution`, `attribution-detail` oder `marketing`.
#' @param shop character. View-Name oder `shopId`.
#' @param start_date character/Date. Startdatum (inklusiv).
#' @param end_date character/Date. Enddatum. Standardmaessig **inklusiv**
#'   interpretiert — die API erwartet exklusiv, deshalb wird intern +1 Tag
#'   gerechnet. Mit `end_inclusive = FALSE` wird der Wert unveraendert
#'   durchgereicht.
#' @param params list. Zusaetzliche Query-Parameter des jeweiligen Reports.
#' @param end_inclusive logical. Siehe `end_date`. Default `TRUE`.
#' @param api_key character.
#' @param max_pages integer. Sicherheitsnetz gegen Endlosschleifen.
#' @param warn_cap logical. Warnen, wenn der serverseitige Zeilen-Cap von
#'   100.000 erreicht wurde (Ergebnis dann abgeschnitten). `FALSE` setzt nur die
#'   rekursive Bisektion, die den Cap selbst aufloest.
#' @return Ein Tibble mit allen Zeilen aller Seiten.
#' @importFrom httr RETRY GET add_headers status_code content
#' @importFrom jsonlite fromJSON
#' @importFrom dplyr bind_rows as_tibble
#' @export
#' @examples \dontrun{
#' get_getklar_report("revenue-and-profit", "Finance View",
#'   "2026-06-01", "2026-06-30",
#'   params = list(dimensions = "calendar_date")
#' )
#' }
get_getklar_report <- function(report,
                               shop,
                               start_date,
                               end_date,
                               params = list(),
                               end_inclusive = TRUE,
                               api_key = .getklar_key(),
                               max_pages = 10000L,
                               warn_cap = TRUE) {
  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)

  if (is.na(start_date) || is.na(end_date)) {
    stop("start_date und end_date muessen gueltige Daten sein ('YYYY-MM-DD').")
  }
  if (end_date < start_date) stop("end_date liegt vor start_date.")

  # API-Semantik: endDate ist exklusiv
  api_end <- if (end_inclusive) end_date + 1 else end_date

  shop_id <- .getklar_shop_id(shop, api_key = api_key)

  query <- c(
    list(
      shopId    = shop_id,
      startDate = format(start_date, "%Y-%m-%d"),
      endDate   = format(api_end, "%Y-%m-%d")
    ),
    params
  )

  # --- Schritt 1: Report anstossen -> dataUrl ------------------------------
  res <- httr::RETRY(
    verb = "GET",
    url = paste0("https://api.getklar.com/v1/public/", report),
    httr::add_headers(`X-API-Key` = api_key),
    query = query,
    times = 3, pause_base = 3, pause_cap = 20
  )

  if (httr::status_code(res) != 200) {
    stop(
      "GetKlar-Report '", report, "' fehlgeschlagen! Status: ",
      httr::status_code(res), " | ",
      httr::content(res, as = "text", encoding = "UTF-8")
    )
  }

  init <- jsonlite::fromJSON(httr::content(res, as = "text", encoding = "UTF-8"))

  if (is.null(init$dataUrl)) {
    stop("Antwort enthaelt kein `dataUrl` — Report-Struktur pruefen.")
  }

  # --- Schritt 2: Seiten abholen ------------------------------------------
  pages <- list()
  url <- init$dataUrl
  i <- 0L

  while (!is.null(url) && i < max_pages) {
    i <- i + 1L

    pres <- httr::RETRY(
      verb = "GET", url = url,
      httr::add_headers(`X-API-Key` = api_key),
      times = 3, pause_base = 3, pause_cap = 20
    )

    if (httr::status_code(pres) != 200) {
      stop(
        "GetKlar-Seitenabruf fehlgeschlagen (Seite ", i, ")! Status: ",
        httr::status_code(pres), " | ",
        httr::content(pres, as = "text", encoding = "UTF-8")
      )
    }

    pg <- jsonlite::fromJSON(
      httr::content(pres, as = "text", encoding = "UTF-8"),
      flatten = TRUE
    )

    if (!is.null(pg$results) && length(pg$results) > 0 && NROW(pg$results) > 0) {
      pages[[length(pages) + 1]] <- pg$results
    }

    url <- pg$nextPage
    if (i %% 10 == 0) message("   ... ", i, " Seiten geladen")
  }

  if (i >= max_pages) {
    warning(
      "max_pages (", max_pages, ") erreicht — Ergebnis moeglicherweise ",
      "unvollstaendig."
    )
  }

  out <- dplyr::bind_rows(pages) |> dplyr::as_tibble()

  # Serverseitiger Zeilen-Cap (verifiziert 07.08.2026: exakt 100.000 Zeilen bei
  # `linear`/`time_decay` fuer einen Monat) — die API meldet das NICHT, sie
  # liefert einfach weniger. Ohne Warnung sieht das aus wie ein Modelleffekt.
  if (warn_cap && nrow(out) >= .GETKLAR_ROW_CAP) {
    warning(
      "GetKlar-Report '", report, "' hat den Zeilen-Cap von ",
      .GETKLAR_ROW_CAP, " erreicht — das Ergebnis ist ABGESCHNITTEN. ",
      "Zeitraum verkleinern (get_getklar_attribution() macht das automatisch)."
    )
  }

  message(sprintf(
    "%s: %d Zeilen aus %d Seite(n) (%s bis %s%s)",
    report, nrow(out), i, start_date, end_date,
    if (end_inclusive) " inkl." else " exkl."
  ))

  out
}


#' @title Trim a Report to the Requested Date Range
#' @description Interner Helper. Die endDate-Semantik der API ist nicht stabil
#'   (mal exklusiv, mal inklusiv), deshalb wird das Ergebnis hier hart auf den
#'   angeforderten Zeitraum beschnitten. Ohne das wandert je nach Serverstand
#'   ein Zusatztag in jede Auswertung.
#' @param df data.frame. Report-Ergebnis.
#' @param date_col character. Name der Datumsspalte.
#' @param from,to Date. Angeforderte Grenzen (beide inklusiv).
#' @return `df`, beschnitten.
#' @keywords internal
.getklar_trim_range <- function(df, date_col, from, to) {
  if (!nrow(df) || !date_col %in% names(df)) {
    return(df)
  }
  d <- as.Date(df[[date_col]])
  drop <- !is.na(d) & (d < from | d > to)
  if (any(drop)) {
    message(sprintf(
      "   %d Zeile(n) ausserhalb %s..%s verworfen (endDate-Semantik)",
      sum(drop), from, to
    ))
  }
  df[!drop, , drop = FALSE]
}

#' Zeilen-Cap der Klar-API pro Report-Abruf (empirisch verifiziert).
#' @keywords internal
.GETKLAR_ROW_CAP <- 100000L

#' Maximale Zeitraumlaenge des `attribution`-Endpunkts in Tagen (verifiziert:
#' 31 Tage -> HTTP 200, 32 Tage -> HTTP 400).
#' @keywords internal
.GETKLAR_ATTRIBUTION_MAX_DAYS <- 31L


#' @title Fetch the GetKlar Revenue & Profit Report
#' @description Der Endpunkt fuer **Umsatzzahlen** — liefert Brutto, Netto,
#'   Retouren, Steuern, Rabatte, COGS und Deckungsbeitraege getrennt
#'   ausgewiesen. Das ist die API-Entsprechung des Revenue-&-Profit-Reports im
#'   Portal und damit die Quelle fuer `portal_kennzahlen.csv`.
#'
#'   **Verifizierte Kennzahlen-Logik** (07.08.2026, Live-Daten):
#'   \itemize{
#'     \item `netRevenue = grossRevenue - taxValue - returnValue` (auf den Cent)
#'     \item `grossRevenue` ist brutto **inkl.** USt., nach Rabatten
#'     \item `date_granularity` steuert die **Refund-Basis** und veraendert
#'       `returnValue`/`netRevenue` massiv: `"order"` bucht Erstattungen auf das
#'       Bestelldatum zurueck, `"event"` auf das Erstattungsdatum. Beispiel
#'       20.06.2026: `returnValue` 150.650 EUR (order) vs. 9.253 EUR (event).
#'       Das ist dieselbe Unterscheidung wie `refund_basis` in
#'       `build_revenue_reconciliation()` — beim Vergleich unbedingt gleich
#'       waehlen.
#'   }
#' @param shop character. View-Name oder `shopId`. Default `"Finance View"`.
#' @param start_date character/Date. Startdatum (inklusiv).
#' @param end_date character/Date. Enddatum (inklusiv, siehe `get_getklar_report()`).
#' @param dimensions character. Bis zu 5 Dimensionen, komma-separiert oder als
#'   Vektor (z. B. `c("calendar_date", "channel_name")`). Default
#'   `"calendar_date"`. Die zurueckgegebenen Spalten `dimension1..5` werden
#'   automatisch auf diese Namen umbenannt.
#' @param date_granularity character. `"order"` (Erstattungen auf Bestelldatum)
#'   oder `"event"` (auf Erstattungsdatum). Default `"order"`.
#' @param debundle integer. `1` splittet Bundles in Komponenten, `0` nicht.
#' @param api_key character.
#' @return Ein Tibble. Kennzahlen u. a. `grossRevenue`, `netRevenue`,
#'   `returnValue`, `taxValue`, `discounts`, `priceReductions`,
#'   `grossMerchandiseValue`, `shippingRevenue`, `cogsValue`, `cm1`, `cm2`,
#'   `logisticsCosts`, `transactionCosts`, `grossOrders`, `netOrders`,
#'   `newCustomers`, `returningCustomers`.
#' @importFrom dplyr rename_with all_of
#' @export
#' @examples \dontrun{
#' # Tagesumsaetze Juni 2026
#' rev <- get_getklar_revenue(start_date = "2026-06-01", end_date = "2026-06-30")
#'
#' # Nach Kanal, Erstattungen auf Erstattungsdatum
#' rev_ch <- get_getklar_revenue(
#'   start_date = "2026-01-01", end_date = "2026-07-31",
#'   dimensions = c("calendar_date", "channel_name"),
#'   date_granularity = "event"
#' )
#' }
get_getklar_revenue <- function(shop = "Finance View",
                                start_date,
                                end_date,
                                dimensions = "calendar_date",
                                date_granularity = c("order", "event"),
                                debundle = 0L,
                                api_key = .getklar_key()) {
  date_granularity <- match.arg(date_granularity)

  dims <- unlist(strsplit(paste(dimensions, collapse = ","), ","))
  dims <- trimws(dims[nzchar(trimws(dims))])

  if (length(dims) > 5) stop("Maximal 5 Dimensionen erlaubt (API-Limit).")

  out <- get_getklar_report(
    report = "revenue-and-profit",
    shop = shop,
    start_date = start_date,
    end_date = end_date,
    params = list(
      dimensions       = paste(dims, collapse = ","),
      date_granularity = date_granularity,
      debundle         = debundle
    ),
    api_key = api_key
  )

  # dimension1..5 auf die angeforderten Namen umbenennen, Leerspalten entfernen
  if (nrow(out) > 0) {
    for (i in seq_along(dims)) {
      col <- paste0("dimension", i)
      if (col %in% names(out)) names(out)[names(out) == col] <- dims[i]
    }
    leer <- paste0("dimension", seq_len(5))
    leer <- intersect(leer, names(out))
    if (length(leer) > 0) out <- out[, setdiff(names(out), leer), drop = FALSE]

    if ("calendar_date" %in% names(out)) {
      out$calendar_date <- as.Date(out$calendar_date)
      out <- .getklar_trim_range(
        out, "calendar_date",
        as.Date(start_date), as.Date(end_date)
      )
    }
  }

  attr(out, "date_granularity") <- date_granularity
  out
}


#' @title Recursively Bisect an Attribution Request Until It Fits the Row Cap
#' @description Interner Helper. Die Klar-API kappt jeden Report still bei
#'   100.000 Zeilen. Der Attributions-Report ist auf Ad-Ebene granular und
#'   reisst dieses Limit bei feingranularen Modellen (`linear`, `time_decay`)
#'   schon innerhalb eines Monats. Diese Funktion holt den Zeitraum und
#'   halbiert ihn rekursiv, solange der Cap erreicht wird — dasselbe Muster wie
#'   `get_paypal_transactions()` gegen das 10.000-Items-Limit.
#'
#'   Verifiziert am 07.08.2026: Juni 2026 mit `metric = "linear"` liefert in
#'   einem Request 100.000 Zeilen / 8.789.111 EUR, halbiert 122.660 Zeilen /
#'   10.841.649 EUR — exakt der Wert von `last_touch`.
#' @param shop character. View-Name oder `shopId`.
#' @param from,to Date. Zeitraumgrenzen (beide inklusiv).
#' @param params list. Modellparameter des Reports.
#' @param api_key character.
#' @param depth integer. Rekursionstiefe (intern).
#' @return Ein Tibble mit allen Zeilen des Zeitraums.
#' @importFrom dplyr bind_rows
#' @keywords internal
.getklar_attribution_bisect <- function(shop, from, to, params, api_key, depth = 0L) {
  out <- get_getklar_report(
    report = "attribution", shop = shop,
    start_date = from, end_date = to,
    params = params, api_key = api_key, warn_cap = FALSE
  )

  if (nrow(out) < .GETKLAR_ROW_CAP) {
    return(out)
  }

  # Cap erreicht -> Zeitraum halbieren. Bei einem einzelnen Tag geht das nicht
  # mehr; dann bleibt nur die Warnung, damit die Luecke nicht still bleibt.
  if (from >= to) {
    warning(
      "Zeilen-Cap an einem einzelnen Tag erreicht (", from,
      ") — dieser Tag ist unvollstaendig. Nicht weiter teilbar."
    )
    return(out)
  }

  mitte <- from + floor(as.numeric(to - from) / 2)
  message(
    strrep(" ", depth * 2), "Cap erreicht (", from, " bis ", to,
    ") — halbiere bei ", mitte
  )

  dplyr::bind_rows(
    .getklar_attribution_bisect(shop, from, mitte, params, api_key, depth + 1L),
    .getklar_attribution_bisect(shop, mitte + 1, to, params, api_key, depth + 1L)
  )
}


#' @title Fetch the GetKlar Attribution Report
#' @description Attributierter Umsatz je Kanal/Kampagne/Adgroup/Ad und Tag.
#'   Der Report ist auf **Ad-Ebene** granular — ein Monat liefert je nach
#'   Modell 60.000 bis >120.000 Zeilen. Da die API still bei 100.000 Zeilen
#'   kappt, halbiert diese Funktion den Zeitraum bei Bedarf **automatisch**
#'   (rekursive Bisektion, s. `.getklar_attribution_bisect()`). Fuer
#'   Kanalvergleiche danach mit `summarise_getklar_revenue(by_channel = TRUE)`
#'   verdichten.
#'
#'   Die Summe ueber alle Kanaele entspricht nur dann dem Gesamtumsatz, wenn
#'   die Modellgewichte je Bestellung auf 1 aufgehen — das gilt **nicht** fuer
#'   `any_click` (jeder Klick erhaelt den vollen Wert). Fuer Gesamtumsaetze ist
#'   ohnehin `get_getklar_revenue()` die richtige Quelle.
#' @param shop character. View-Name oder `shopId`. Default `"Marketing View"`.
#' @param start_date character/Date. Startdatum (inklusiv).
#' @param end_date character/Date. Enddatum (inklusiv).
#' @param metric character. Attributionsmodell: `last_touch`, `first_touch`,
#'   `data_driven`, `linear`, `any_click`, `any_click_unique`, `u_shape`,
#'   `time_decay`, `marketing_mix`.
#' @param window character. Lookback: `unlimited`, `1_day`, `7_day`, `28_day`.
#' @param date_breakdown character. `order` (Bestelldatum) oder `touch`
#'   (Touchpoint-Datum). Entspricht `basis = conversion|click` im
#'   Adtribute-Cube `daily_channel.rds`.
#' @param api_key character.
#' @return Ein Tibble mit `channelName`, `date`, `campaignName`, `adGroupName`,
#'   `adName` sowie `orders`, `grossRevenue`, `netRevenue`, `cost`, `clicks`,
#'   `impressions`, `cm1`, `cm2`, `clv_30/60/90`, `ncGrossRevenue`,
#'   `rcGrossRevenue` u. a. Die gewaehlten Modellparameter haengen als Spalten an.
#' @export
#' @examples \dontrun{
#' att <- get_getklar_attribution(start_date = "2026-06-01", end_date = "2026-06-30")
#' summarise_getklar_revenue(att, grain = "total", by_channel = TRUE)
#' }
get_getklar_attribution <- function(shop = "Marketing View",
                                    start_date,
                                    end_date,
                                    metric = c(
                                      "last_touch", "first_touch", "data_driven",
                                      "linear", "any_click", "any_click_unique",
                                      "u_shape", "time_decay", "marketing_mix"
                                    ),
                                    window = c("unlimited", "1_day", "7_day", "28_day"),
                                    date_breakdown = c("order", "touch"),
                                    api_key = .getklar_key()) {
  metric <- match.arg(metric)
  window <- match.arg(window)
  date_breakdown <- match.arg(date_breakdown)

  from <- as.Date(start_date)
  to <- as.Date(end_date)
  if (is.na(from) || is.na(to)) stop("Ungueltiges Datum ('YYYY-MM-DD').")
  if (to < from) stop("end_date liegt vor start_date.")

  params <- list(metric = metric, window = window, date_breakdown = date_breakdown)

  # Zwei unabhaengige Grenzen: (1) harte 31-Tage-Schranke des Endpunkts —
  # deshalb VOR dem Abruf in Fenster schneiden; (2) 100.000-Zeilen-Cap, den
  # .getklar_attribution_bisect() innerhalb jedes Fensters aufloest.
  starts <- seq(from, to, by = paste(.GETKLAR_ATTRIBUTION_MAX_DAYS, "days"))

  if (length(starts) > 1) {
    message(sprintf(
      "Zeitraum > %d Tage -> %d Fenster (Endpunkt-Limit)",
      .GETKLAR_ATTRIBUTION_MAX_DAYS, length(starts)
    ))
  }

  out <- lapply(starts, function(w_from) {
    w_to <- min(w_from + .GETKLAR_ATTRIBUTION_MAX_DAYS - 1, to)
    .getklar_attribution_bisect(
      shop = shop, from = w_from, to = w_to,
      params = params, api_key = api_key
    )
  }) |>
    dplyr::bind_rows()

  if (nrow(out) > 0) {
    if ("date" %in% names(out)) out$date <- as.Date(out$date)
    out$metric <- metric
    out$window <- window
    out$date_breakdown <- date_breakdown
  }

  out
}


#' @title Aggregate a GetKlar Report to Period Totals
#' @description Verdichtet die Rueckgabe von `get_getklar_revenue()` oder
#'   `get_getklar_attribution()` auf Tag/Monat/Jahr/Gesamt — die mit
#'   `portal_kennzahlen.csv` bzw. `build_portal_revenue_models()` vergleichbare
#'   Ebene. Erkennt die Datumsspalte selbst (`calendar_date` oder `date`) und
#'   summiert nur Kennzahlen, die tatsaechlich vorhanden sind.
#' @param df data.frame. Rueckgabe einer der beiden Abruffunktionen.
#' @param grain character. `month`, `day`, `year` oder `total`.
#' @param by_channel logical. Zusaetzlich nach Kanal gruppieren
#'   (`channelName` bzw. `channel_name`).
#' @return Ein Tibble mit `period` (+ optional Kanal) und den Summen der
#'   vorhandenen Kennzahlen. Enthaelt `grossRevenue` und `netRevenue`, wird
#'   `implied_deductions` (= gross - net, also Steuern + Retouren) ergaenzt.
#' @importFrom dplyr group_by summarise across all_of
#' @export
#' @examples \dontrun{
#' rev <- get_getklar_revenue(start_date = "2026-01-01", end_date = "2026-07-31")
#' summarise_getklar_revenue(rev, grain = "month")
#' }
summarise_getklar_revenue <- function(df,
                                      grain = c("month", "day", "year", "total"),
                                      by_channel = FALSE) {
  grain <- match.arg(grain)

  if (!is.data.frame(df) || nrow(df) == 0) {
    stop("`df` ist leer oder kein data.frame.")
  }

  date_col <- intersect(c("calendar_date", "date"), names(df))
  if (length(date_col) == 0 && grain != "total") {
    stop(
      "Keine Datumsspalte (`calendar_date`/`date`) gefunden — ",
      "mit grain = 'total' aggregieren oder `calendar_date` als Dimension anfordern."
    )
  }

  d <- if (length(date_col) > 0) as.Date(df[[date_col[1]]]) else as.Date(NA)

  df$period <- switch(grain,
    day   = d,
    month = as.Date(format(d, "%Y-%m-01")),
    year  = as.Date(format(d, "%Y-01-01")),
    total = as.Date(NA)
  )

  metric_cols <- intersect(
    c(
      "grossOrders", "netOrders", "grossItems", "netItems",
      "grossRevenue", "netRevenue", "returnValue", "discounts",
      "priceReductions", "grossMerchandiseValue", "shippingRevenue",
      "taxValue", "cogsValue", "cm1", "logisticsCosts", "transactionCosts",
      "cm2", "newCustomers", "returningCustomers",
      "orders", "nc", "rc", "cost", "clicks", "impressions",
      "ncGrossRevenue", "rcGrossRevenue", "ncNetRevenue", "rcNetRevenue"
    ),
    names(df)
  )

  if (length(metric_cols) == 0) stop("Keine bekannten Kennzahlen-Spalten gefunden.")

  chan_col <- intersect(c("channelName", "channel_name"), names(df))
  group_cols <- if (by_channel && length(chan_col) > 0) c("period", chan_col[1]) else "period"

  out <- df |>
    dplyr::group_by(dplyr::across(dplyr::all_of(group_cols))) |>
    dplyr::summarise(
      dplyr::across(dplyr::all_of(metric_cols), \(x) sum(as.numeric(x), na.rm = TRUE)),
      .groups = "drop"
    )

  if (all(c("grossRevenue", "netRevenue") %in% names(out))) {
    out$implied_deductions <- out$grossRevenue - out$netRevenue
  }

  out
}
