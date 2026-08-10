#' @title Shopify-Umsatz in den Rechenmodellen der Portale (GetKlar, Adtribute)
#'
#' @description Aggregiert die Shopify-Master-DuckDB je Periode und rechnet den
#' Umsatz zusätzlich in den beiden Definitionen nach, die GetKlar und Adtribute
#' laut Dashboard-Abgleich verwenden. Damit lassen sich die abgeschriebenen
#' Portal-Kennzahlen Zeile für Zeile gegen die eigene Source of Truth stellen.
#'
#' Ergänzt \code{\link{build_revenue_reconciliation}}: dort stehen die
#' Bausteine einer Periode, hier die fertigen Portal-Modelle plus die
#' Order-Zähl-Varianten.
#'
#' @section Adtribute — dokumentierte Definition (exakt nachgebaut):
#' Adtribute hat die Spaltendefinitionen offengelegt. Auf Conversion-Ebene:
#' \preformatted{
#' Net Revenue    = Gross Revenue + Refunds - Taxes
#' Gross Revenue  = Order Filter != "Exclude" ? Shopify Order Gross Revenue : 0
#' Refunds        = (financial_status = "refunded" OR is_canceled)
#'                    ? -Shopify Order Gross Revenue
#'                    : -Refunds
#' Taxes          = Gross Taxes + Refunded Taxes
#' Gross Taxes    = Order Filter != "Exclude" ? Taxes : 0
#' Refunded Taxes = (financial_status = "refunded" OR is_canceled)
#'                    ? -Taxes
#'                    : -(Refunds x effektiver Steuersatz)
#' Order Filter   = SUM(gross_revenue) over order = 0 ? "Exclude" : "Include"
#' }
#' Übersetzt auf unsere Spalten (Layer 3 der Definition nennt die Shopify-Felder):
#' \code{Shopify Order Gross Revenue = total_price}, \code{Taxes = total_tax},
#' \code{Refunds = returned_amount}, \code{effektiver Steuersatz = total_tax / total_price}.
#'
#' Drei Konsequenzen, die eine naive \code{brutto - USt - Erstattung}-Rechnung
#' \strong{nicht} abbildet:
#' \enumerate{
#'   \item \strong{Zwangsnull:} Orders mit \code{financial_status = 'refunded'}
#'     oder gesetztem \code{cancelled_at} bekommen als Erstattung den
#'     \emph{vollen Bruttowert} zugewiesen, nicht den tatsächlich erstatteten
#'     Betrag. Ihr Netto ist damit exakt 0 — unabhängig davon, was Shopify an
#'     Refund gebucht hat. In den Adtribute-Parquets ist das zeilenweise
#'     nachweisbar: für alle 7.477.420 betroffenen Zeilen gilt
#'     \code{refunds = -gross_revenue} und \code{refunded_taxes = -gross_taxes},
#'     ohne eine einzige Ausnahme.
#'   \item \strong{Order Filter:} Orders mit \code{total_price = 0} werden
#'     komplett ausgeschlossen. Das erklärt die Order-Zahl des Dashboards:
#'     \code{orders_all - null_orders} trifft sie auf 0,02–0,4 \% genau.
#'   \item \strong{Hergeleitete Erstattungssteuer:} Adtribute rechnet
#'     \code{Erstattung x (total_tax / total_price)} statt Shopifys tatsächliche
#'     \code{returned_tax} aus \code{refund_line_items[].total_tax} zu nehmen.
#'     Historisch deckungsgleich (Jahresabweichung unter 50 €), ab 2026 driftet
#'     es messbar auseinander — siehe \code{refund_tax_gap}.
#' }
#'
#' @section GetKlar — empirisch bestimmt:
#' Für GetKlar liegt keine belastbare Spaltendefinition vor (die gelieferte
#' Formel enthält keinen Erstattungsterm, das Dashboard zieht Erstattungen aber
#' ab). Aus dem Abgleich der Jahreswerte 2022–2026 folgt:
#' \describe{
#'   \item{Brutto}{\code{SUM(total_price)} \strong{ohne} stornierte Orders —
#'     Treffer unter 0,01 \% für 2022–2025.}
#'   \item{Netto}{\code{brutto - (total_tax - returned_tax) - returned_amount},
#'     also \strong{ohne} Zwangsnull-Regel: vollerstattete (nicht stornierte)
#'     Orders gehen mit ihrem tatsächlichen Refund ein.}
#'   \item{Order-Zählung}{ohne Storno \emph{und} ohne vollerstattete
#'     (\code{financial_status = 'refunded'}) — trifft die Dashboard-Zahl
#'     2022–2024 auf 1–3 Orders genau.}
#' }
#'
#' @section Warum die Netto-Werte trotz großer Brutto-Differenz zusammenpassen:
#' Stornierte Orders sind fast vollständig erstattet. Sie erhöhen also
#' \code{brutto} und \code{erstattungen} um praktisch denselben Betrag und
#' heben sich im Netto auf. Deshalb unterscheiden sich die Brutto- und
#' Refund-Spalten der Portale um Millionen, die Netto-Spalten aber nur um
#' rund ein Prozent. Wer Portale vergleichen will, muss das Netto vergleichen —
#' oder Brutto und Erstattungen immer gemeinsam.
#'
#' @param dbdir Character. Pfad zur Shopify-Master-DuckDB.
#' @param date_from Date/Character (YYYY-MM-DD) oder NULL. Untere Grenze auf \code{created_at} (inklusiv).
#' @param date_to Date/Character (YYYY-MM-DD) oder NULL. Obere Grenze auf \code{created_at} (exklusiv).
#' @param grain Character. \code{"year"}, \code{"month"}, \code{"week"} oder \code{"day"}.
#'
#' @return Ein Tibble mit einer Zeile je Periode:
#'   \describe{
#'     \item{period}{Periodenbeginn als Date}
#'     \item{orders_all, orders_excl_cancelled, orders_excl_cancelled_refunded}{Order-Zähl-Varianten}
#'     \item{orders_adtribute_model}{Orders nach Order Filter (ohne \code{total_price = 0})}
#'     \item{orders_zero_price}{Die vom Order Filter verworfenen Orders}
#'     \item{brutto_all, brutto_excl_cancelled}{\code{SUM(total_price)}, inkl. USt.}
#'     \item{discounts_true, discounts_line_level_column}{Gewährte Rabatte (Order-Level,
#'       verbindlich) gegen die unvollständige Positions-Spalte \code{discount_amount}}
#'     \item{tax_gross_all, tax_gross_excl_cancelled}{\code{SUM(total_tax)}}
#'     \item{refund_tax_all, refund_tax_excl_cancelled}{\code{SUM(returned_tax)} — Shopifys tatsächliche Erstattungssteuer}
#'     \item{tax_net_all, tax_net_excl_cancelled}{Steuer abzüglich erstatteter Steuer}
#'     \item{refunds_all, refunds_excl_cancelled}{\code{SUM(returned_amount)}, brutto inkl. USt.}
#'     \item{ad_gross_revenue, ad_gross_taxes, ad_refunds, ad_refunded_taxes, ad_taxes}{
#'       Die Adtribute-Spalten exakt nach dokumentierter Definition (Beträge als
#'       positive Magnituden; Adtribute speichert \code{refunds} und
#'       \code{refunded_taxes} negativ)}
#'     \item{netto_adtribute_exact}{Netto nach dokumentierter Adtribute-Definition — die maßgebliche Zahl}
#'     \item{netto_adtribute_naiv}{Naive Variante ohne Zwangsnull-Regel, nur zum Vergleich}
#'     \item{netto_getklar_model}{Portal-Netto in der GetKlar-Definition (ohne Storno)}
#'     \item{forced_zero_orders, forced_zero_brutto, forced_zero_refund_actual, forced_zero_gap}{
#'       Der Zwangsnull-Block. \code{forced_zero_gap} ist Umsatz, den Adtribute als
#'       vollständig erstattet behandelt, für den Shopify aber \emph{keinen} oder nur
#'       einen Teil-Refund gebucht hat — der Kandidat für fehlgeschlagene
#'       Zahlungsdienstleister-Erstattungen}
#'     \item{refund_tax_derived, refund_tax_gap, refund_tax_gap_orders}{
#'       Adtributes hergeleitete Erstattungssteuer, ihre Abweichung von Shopifys
#'       Ist-Wert und die Zahl betroffener Orders}
#'     \item{cancelled_brutto, cancelled_refunds, cancelled_netto_effekt}{Der Storno-Block isoliert}
#'   }
#'
#' @examples \dontrun{
#' # Jahreswerte in beiden Portal-Definitionen
#' mod <- build_portal_revenue_models(grain = "year")
#'
#' # Gegen die abgeschriebenen Dashboard-Zahlen stellen
#' portal <- utils::read.csv2(
#'   "~/git/dieseoR/scripts/analysen/umsatz_reconciliation/portal_kennzahlen.csv",
#'   dec = "."
#' )
#' }
#'
#' @seealso \code{\link{build_revenue_reconciliation}}
#'
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom dplyr mutate arrange
#' @importFrom tibble as_tibble
#' @export
build_portal_revenue_models <- function(dbdir = "~/data/shopify/shopify.duckdb",
                                        date_from = NULL,
                                        date_to = NULL,
                                        grain = c("year", "month", "week", "day")) {
  grain <- match.arg(grain)

  db_path <- path.expand(dbdir)
  if (!file.exists(db_path)) {
    stop("DuckDB nicht gefunden: ", db_path, call. = FALSE)
  }

  as_iso <- function(x, arg) {
    if (is.null(x)) {
      return(NULL)
    }
    d <- suppressWarnings(as.Date(x))
    if (is.na(d)) stop(sprintf("'%s' ist kein gueltiges Datum: %s", arg, x), call. = FALSE)
    format(d, "%Y-%m-%d")
  }
  from_iso <- as_iso(date_from, "date_from")
  to_iso <- as_iso(date_to, "date_to")

  where_sql <- paste(c(
    "created_at IS NOT NULL",
    if (!is.null(from_iso)) sprintf("created_at >= DATE '%s'", from_iso),
    if (!is.null(to_iso)) sprintf("created_at < DATE '%s'", to_iso)
  ), collapse = " AND ")

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # Stufe 1: Order-Grain. Order-Level-Felder wiederholen sich ueber die
  # Item-Zeilen -> any_value(); Refund-Felder sind Item-Grain -> SUM().
  # Stufe 2: Adtribute-Flags und -Groessen je Order nach dokumentierter Definition.
  # Stufe 3: Aggregation auf die Periode.
  q <- sprintf("
    WITH ord AS (
      SELECT order_id,
             any_value(created_at)          AS created_at,
             any_value(total_price)         AS total_price,
             any_value(total_tax)           AS total_tax,
             any_value(total_discounts)     AS total_discounts,
             any_value(cancellation_status) AS cancelled,
             any_value(financial_status)    AS financial_status,
             SUM(returned_amount)           AS returned_amount,
             SUM(returned_tax)              AS returned_tax,
             SUM(discount_amount)           AS discount_amount_line_only
      FROM orders
      WHERE %s
      GROUP BY order_id
    ),
    adt AS (
      SELECT *,
             -- Order Filter: SUM(gross_revenue) over order = 0 -> 'Exclude'
             (total_price = 0)                            AS ad_excluded,
             -- Zwangsnull-Schalter der Definition
             (financial_status = 'refunded' OR cancelled)  AS ad_forced_zero,
             -- effektiver Steuersatz der Order (Adtribute: SAFE_DIVIDE)
             CASE WHEN total_price > 0 THEN total_tax / total_price ELSE 0 END AS eff_tax_rate
      FROM ord
    ),
    adt2 AS (
      SELECT *,
             CASE WHEN ad_excluded THEN 0 ELSE total_price END AS ad_gross_revenue,
             CASE WHEN ad_excluded THEN 0 ELSE total_tax   END AS ad_gross_taxes,
             -- Erstattung: bei Zwangsnull der volle Bruttowert, sonst der Ist-Refund
             CASE WHEN ad_excluded   THEN 0
                  WHEN ad_forced_zero THEN total_price
                  ELSE returned_amount END                    AS ad_refunds,
             -- Erstattungssteuer: bei Zwangsnull die volle Steuer,
             -- sonst Erstattung x effektiver Steuersatz (NICHT Shopifys returned_tax)
             CASE WHEN ad_excluded   THEN 0
                  WHEN ad_forced_zero THEN total_tax
                  ELSE returned_amount * eff_tax_rate END      AS ad_refunded_taxes
      FROM adt
    )
    SELECT
      CAST(date_trunc('%s', created_at) AS DATE) AS period,

      COUNT(*)                                                        AS orders_all,
      SUM(CASE WHEN NOT cancelled THEN 1 ELSE 0 END)                  AS orders_excl_cancelled,
      SUM(CASE WHEN NOT cancelled AND financial_status <> 'refunded'
               THEN 1 ELSE 0 END)                                     AS orders_excl_cancelled_refunded,
      SUM(CASE WHEN ad_excluded THEN 1 ELSE 0 END)                    AS orders_zero_price,
      SUM(CASE WHEN NOT ad_excluded THEN 1 ELSE 0 END)                AS orders_adtribute_model,

      SUM(total_price)                                                AS brutto_all,
      SUM(CASE WHEN NOT cancelled THEN total_price ELSE 0 END)        AS brutto_excl_cancelled,

      SUM(total_tax)                                                  AS tax_gross_all,
      SUM(CASE WHEN NOT cancelled THEN total_tax ELSE 0 END)          AS tax_gross_excl_cancelled,

      -- Rabatte: Order-Level ist die Wahrheit, die Positions-Spalte unterschaetzt
      -- (siehe clean_up_shopify(): discount_amount kennt discount_allocations nicht)
      SUM(total_discounts)                                            AS discounts_true,
      SUM(discount_amount_line_only)                                  AS discounts_line_level_column,

      SUM(returned_tax)                                               AS refund_tax_all,
      SUM(CASE WHEN NOT cancelled THEN returned_tax ELSE 0 END)       AS refund_tax_excl_cancelled,

      SUM(returned_amount)                                            AS refunds_all,
      SUM(CASE WHEN NOT cancelled THEN returned_amount ELSE 0 END)    AS refunds_excl_cancelled,

      -- ==== Adtribute exakt nach dokumentierter Definition ====
      SUM(ad_gross_revenue)                                           AS ad_gross_revenue,
      SUM(ad_gross_taxes)                                             AS ad_gross_taxes,
      SUM(ad_refunds)                                                 AS ad_refunds,
      SUM(ad_refunded_taxes)                                          AS ad_refunded_taxes,

      -- ==== Diagnose: Zwangsnull-Block ====
      SUM(CASE WHEN ad_forced_zero AND NOT ad_excluded THEN 1 ELSE 0 END)
                                                                      AS forced_zero_orders,
      SUM(CASE WHEN ad_forced_zero AND NOT ad_excluded THEN total_price ELSE 0 END)
                                                                      AS forced_zero_brutto,
      SUM(CASE WHEN ad_forced_zero AND NOT ad_excluded THEN returned_amount ELSE 0 END)
                                                                      AS forced_zero_refund_actual,

      -- ==== Diagnose: hergeleitete vs. tatsaechliche Erstattungssteuer ====
      -- nur die NICHT-Zwangsnull-Orders, dort weichen die beiden Wege ab
      SUM(CASE WHEN NOT ad_forced_zero AND NOT ad_excluded
               THEN returned_amount * eff_tax_rate ELSE 0 END)        AS refund_tax_derived_nonforced,
      SUM(CASE WHEN NOT ad_forced_zero AND NOT ad_excluded
               THEN returned_tax ELSE 0 END)                          AS refund_tax_actual_nonforced,
      SUM(CASE WHEN NOT ad_forced_zero AND NOT ad_excluded
                AND returned_amount > 0
                AND abs(returned_amount * eff_tax_rate - returned_tax) > 0.02
               THEN 1 ELSE 0 END)                                     AS refund_tax_gap_orders,
      -- gleiche Population fuer die Steuersatz-Gegenueberstellung
      SUM(CASE WHEN NOT ad_forced_zero AND NOT ad_excluded
               THEN total_price ELSE 0 END)                           AS brutto_nonforced,
      SUM(CASE WHEN NOT ad_forced_zero AND NOT ad_excluded
               THEN total_tax ELSE 0 END)                             AS tax_gross_nonforced,
      SUM(CASE WHEN NOT ad_forced_zero AND NOT ad_excluded
               THEN returned_amount ELSE 0 END)                       AS refunds_nonforced,

      -- Storno-Block isoliert: zeigt, warum sich die Portale im Netto einig sind
      SUM(CASE WHEN cancelled THEN total_price ELSE 0 END)            AS cancelled_brutto,
      SUM(CASE WHEN cancelled THEN returned_amount ELSE 0 END)        AS cancelled_refunds
    FROM adt2
    GROUP BY 1
    ORDER BY 1", where_sql, grain)

  out <- DBI::dbGetQuery(con, q) |>
    tibble::as_tibble() |>
    dplyr::mutate(
      # Die Steuerspalte der Portale ist netto: berechnete minus erstattete USt.
      tax_net_all = tax_gross_all - refund_tax_all,
      tax_net_excl_cancelled = tax_gross_excl_cancelled - refund_tax_excl_cancelled,

      # --- Adtribute exakt: Taxes = Gross Taxes + Refunded Taxes (letztere negativ) ---
      ad_taxes = ad_gross_taxes - ad_refunded_taxes,
      # Net Revenue = Gross Revenue + Refunds - Taxes  (Refunds negativ)
      netto_adtribute_exact = ad_gross_revenue - ad_refunds - ad_taxes,

      # --- Naive Variante ohne Zwangsnull-Regel, nur als Vergleichsmassstab ---
      netto_adtribute_naiv = brutto_all - tax_net_all - refunds_all,

      # --- GetKlar: ohne Storno, mit tatsaechlichen Erstattungen ---
      netto_getklar_model = brutto_excl_cancelled - tax_net_excl_cancelled - refunds_excl_cancelled,

      # --- Diagnosen ---
      # Umsatz, den Adtribute als voll erstattet behandelt, den Shopify aber nicht
      # (oder nur teilweise) als Refund gebucht hat:
      forced_zero_gap = forced_zero_brutto - forced_zero_refund_actual,
      refund_tax_derived = refund_tax_derived_nonforced,
      refund_tax_gap = refund_tax_derived_nonforced - refund_tax_actual_nonforced,
      cancelled_netto_effekt = netto_adtribute_naiv - netto_getklar_model
    ) |>
    dplyr::arrange(period)

  attr(out, "meta") <- list(
    dbdir = db_path,
    date_from = from_iso,
    date_to = to_iso,
    grain = grain,
    adtribute = paste(
      "dokumentierte Definition: Net = Gross + Refunds - Taxes;",
      "Order Filter (total_price = 0 raus);",
      "Zwangsnull bei financial_status='refunded' ODER cancelled;",
      "Refunded Taxes = Refund x (total_tax/total_price)"
    ),
    getklar = "empirisch: ohne Storno, netto = brutto - (total_tax - returned_tax) - returned_amount",
    generated_at = Sys.time()
  )

  out
}
