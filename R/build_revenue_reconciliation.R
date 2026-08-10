#' @title Baukasten-Tabelle zur Rekonstruktion der Portal-Nettoumsaetze (GetKlar, Adtribute)
#'
#' @description Aggregiert die Shopify-Master-DuckDB auf Tages-, Wochen- oder
#' Monatsebene und liefert alle Bausteine, aus denen die Portale ihre
#' Umsatzkennzahlen zusammensetzen (Brutto vor Rabatt, echte Rabatte, Versand,
#' USt., Erstattungen, Return-Fees) — plus die daraus abgeleiteten
#' Nettoumsatz-Varianten.
#'
#' Die Aggregation laeuft vollstaendig in DuckDB (zweistufig: erst Order-Grain,
#' dann Perioden-Grain), es wird nie eine Master-Tabelle in den RAM gezogen.
#'
#' @section Nachgebildete GetKlar-Formel:
#' GetKlar rechnet laut Vorgabe:
#' \preformatted{
#' net_revenue = _original_gross_revenue   # Line Items + Versand, nach Rabatt, inkl. USt.
#'             - sales_tax                 # USt. aus den Line-Item-tax_lines
#'             - shipping_tax              # USt. auf Versand
#'             - duties                    # Zoll
#' }
#' Auf den Pammys-Rohdaten (verifiziert 27.07.2026) gilt:
#' \itemize{
#'   \item \code{_original_gross_revenue} == Shopify \code{total_price}. Die Identitaet
#'         \code{total_price = SUM(quantity * price) - total_discounts + shipping}
#'         haelt exakt fuer 1.594.892 / 1.594.892 Orders ab 2025-01-01.
#'   \item \code{sales_tax + shipping_tax} == Shopify \code{total_tax} (Order-Level).
#'         Verifiziert auf Rohdaten-Stichprobe: 2.453 / 2.456 Orders < 0,02 EUR Abweichung.
#'         Der Versandsteuer-Anteil liegt bei ca. 1 \% der Gesamtsteuer.
#'   \item \code{duties} ist strukturell 0: \code{original_total_duties_set} und
#'         \code{current_total_duties_set} sind in allen geprueften Chunks NULL.
#' }
#' Daraus folgt \code{net_revenue_getklar = total_price - total_tax}.
#'
#' @section Wichtige Annahmen (bitte fachlich gegen GetKlar pruefen):
#' \enumerate{
#'   \item \strong{Original statt Current:} Das Praefix \code{_original_} in
#'         \code{_original_gross_revenue} wird so gelesen, dass GetKlar den
#'         Bestellwert zum Bestellzeitpunkt nutzt, also \emph{ohne} Abzug spaeterer
#'         Erstattungen. \code{net_revenue_getklar_less_refunds} liefert die
#'         Gegenvariante. Die Differenz ist der groesste Einzelhebel im Vergleich
#'         zu Adtribute (Adtribute fuehrt \code{refunds} als eigenes Feld).
#'   \item \strong{Datumsbasis:} Zuordnung ueber \code{created_at} (Bestelldatum).
#'         Erstattungen koennen per \code{refund_basis} alternativ auf ihr
#'         Erstattungsdatum gelegt werden.
#'   \item \strong{Storno:} Stornierte Orders behalten in Shopify ihren
#'         \code{total_price}. Ueber \code{include_cancelled} steuerbar; die Spalte
#'         \code{net_revenue_of_cancelled} macht den Effekt in jedem Fall sichtbar.
#' }
#'
#' @section Bekannte Datenluecken (nicht in der Master-DuckDB):
#' \itemize{
#'   \item \code{discount_amount} (Line-Item-Grain) stammt aus
#'         \code{line_items.total_discount} und enthaelt \strong{nur} Rabatte auf
#'         Positionsebene. Order-Level-Rabattcodes verteilt Shopify ueber
#'         \code{line_items.discount_allocations} — dort fehlen 76–89 \% des
#'         Rabattvolumens. Die Spalte \code{discounts_line_level_column} und
#'         \code{discount_gap} quantifizieren das; gerechnet wird ausschliesslich
#'         mit \code{discounts_true} (= Order-Level \code{total_discounts}).
#'   \item \code{current_total_price}, \code{current_total_tax},
#'         \code{current_subtotal_price}, \code{subtotal_price} und der
#'         Versandsteuer-Split sind nicht extrahiert. Wird eine Current-Value-Logik
#'         eines Portals gebraucht, muss \code{clean_up_shopify()} erweitert werden.
#'   \item Versand-Erstattungen (\code{order_adjustments.kind == 'shipping_refund'})
#'         werden in \code{clean_up_shopify()} bewusst verworfen und fehlen daher
#'         in \code{refunded_gross_incl_vat}.
#' }
#'
#' @param dbdir Character. Pfad zur Shopify-Master-DuckDB.
#' @param date_from Date oder Character (YYYY-MM-DD). Untere Grenze auf \code{created_at} (inklusiv).
#' @param date_to Date oder Character (YYYY-MM-DD) oder NULL. Obere Grenze auf \code{created_at} (exklusiv).
#' @param grain Character. \code{"day"}, \code{"week"} oder \code{"month"}.
#' @param refund_basis Character. \code{"order_date"} legt Erstattungen auf das
#'   Bestelldatum, \code{"refund_date"} auf \code{first_refund_datetime}.
#' @param include_cancelled Logical. Stornierte Orders mitzaehlen?
#'
#' @return Ein Tibble mit einer Zeile je Periode. Kennzahlen-Spalten:
#'   \code{orders}, \code{orders_cancelled}, \code{units},
#'   \code{gross_before_discount_incl_vat}, \code{discounts_true},
#'   \code{discounts_line_level_column}, \code{discount_gap},
#'   \code{shipping_incl_vat}, \code{gross_after_discount_incl_vat},
#'   \code{vat_total}, \code{refunded_gross_incl_vat}, \code{refunded_vat},
#'   \code{refunded_units}, \code{return_fees},
#'   \code{net_revenue_getklar}, \code{net_revenue_getklar_less_refunds},
#'   \code{net_revenue_of_cancelled}.
#'
#' @examples \dontrun{
#' # GetKlar-Nettoumsatz je Monat 2026, Erstattungen auf Bestelldatum
#' rec <- build_revenue_reconciliation(date_from = "2026-01-01", grain = "month")
#'
#' # Taggenau, Erstattungen auf ihr Erstattungsdatum (Timing-Theorie testen)
#' rec_day <- build_revenue_reconciliation(
#'   date_from = "2026-05-01", date_to = "2026-07-01",
#'   grain = "day", refund_basis = "refund_date"
#' )
#' }
#'
#' @importFrom DBI dbConnect dbDisconnect dbGetQuery
#' @importFrom duckdb duckdb
#' @importFrom dplyr full_join arrange mutate select across all_of coalesce
#' @importFrom tibble as_tibble
#' @export
build_revenue_reconciliation <- function(dbdir = "~/data/shopify/shopify.duckdb",
                                         date_from = "2026-01-01",
                                         date_to = NULL,
                                         grain = c("day", "week", "month"),
                                         refund_basis = c("order_date", "refund_date"),
                                         include_cancelled = TRUE) {
  grain <- match.arg(grain)
  refund_basis <- match.arg(refund_basis)

  db_path <- path.expand(dbdir)
  if (!file.exists(db_path)) {
    stop("DuckDB nicht gefunden: ", db_path, call. = FALSE)
  }

  # --- Datumsgrenzen validieren (kein blindes String-Einsetzen in SQL) ---
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

  where_created <- c(
    "created_at IS NOT NULL",
    if (!is.null(from_iso)) sprintf("created_at >= DATE '%s'", from_iso),
    if (!is.null(to_iso)) sprintf("created_at < DATE '%s'", to_iso),
    if (!include_cancelled) "cancellation_status IS NOT TRUE"
  )
  where_created <- paste(where_created, collapse = " AND ")

  bucket <- function(col) sprintf("CAST(date_trunc('%s', %s) AS DATE)", grain, col)

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  # ------------------------------------------------------------------
  # Stufe 1: Order-Grain. Order-Level-Felder sind ueber die Item-Zeilen
  # wiederholt -> any_value(); Item-Level-Felder -> SUM().
  # ------------------------------------------------------------------
  order_cte <- sprintf("
    SELECT
      order_id,
      any_value(created_at)             AS created_at,
      any_value(first_refund_datetime)  AS first_refund_datetime,
      any_value(cancellation_status)    AS cancelled,
      any_value(total_price)            AS total_price,
      any_value(total_tax)              AS total_tax,
      any_value(total_discounts)        AS total_discounts,
      any_value(shipping_charges)       AS shipping_charges,
      any_value(return_fees)            AS return_fees,
      SUM(item_gross_revenue)           AS li_gross,
      SUM(discount_amount)              AS li_discount_only,
      SUM(quantity)                     AS units,
      SUM(returned_amount)              AS refunded_gross_incl_vat,
      SUM(returned_tax)                 AS refunded_vat,
      SUM(returned_quantity)            AS refunded_units
    FROM orders
    WHERE %s
    GROUP BY order_id", where_created)

  # ------------------------------------------------------------------
  # Stufe 2a: Umsatz-Bausteine je Periode (Basis: Bestelldatum)
  # ------------------------------------------------------------------
  refund_cols_here <- if (refund_basis == "order_date") "
      SUM(refunded_gross_incl_vat)                        AS refunded_gross_incl_vat,
      SUM(refunded_vat)                                   AS refunded_vat,
      SUM(refunded_units)                                 AS refunded_units,
      SUM(return_fees)                                    AS return_fees," else ""

  q_rev <- sprintf("
    WITH ord AS (%s)
    SELECT
      %s                                                  AS period,
      COUNT(*)                                            AS orders,
      SUM(CASE WHEN cancelled THEN 1 ELSE 0 END)          AS orders_cancelled,
      SUM(units)                                          AS units,
      SUM(li_gross)                                       AS gross_before_discount_incl_vat,
      SUM(total_discounts)                                AS discounts_true,
      SUM(li_discount_only)                               AS discounts_line_level_column,
      SUM(shipping_charges)                               AS shipping_incl_vat,
      SUM(total_price)                                    AS gross_after_discount_incl_vat,
      SUM(total_tax)                                      AS vat_total,
      %s
      SUM(CASE WHEN cancelled THEN total_price - total_tax ELSE 0 END)
                                                          AS net_revenue_of_cancelled
    FROM ord
    GROUP BY 1
    ORDER BY 1", order_cte, bucket("created_at"), refund_cols_here)

  rev <- DBI::dbGetQuery(con, q_rev)

  # ------------------------------------------------------------------
  # Stufe 2b: Erstattungen auf Erstattungsdatum. Bewusst OHNE created_at-Filter,
  # damit Refunds von Orders aus der Vorperiode korrekt in der Periode landen,
  # in der sie erstattet wurden.
  # ------------------------------------------------------------------
  if (refund_basis == "refund_date") {
    where_refund <- c(
      "first_refund_datetime IS NOT NULL",
      if (!is.null(from_iso)) sprintf("first_refund_datetime >= DATE '%s'", from_iso),
      if (!is.null(to_iso)) sprintf("first_refund_datetime < DATE '%s'", to_iso),
      if (!include_cancelled) "cancellation_status IS NOT TRUE"
    )
    q_ref <- sprintf(
      "
      WITH ord AS (
        SELECT
          order_id,
          any_value(first_refund_datetime) AS first_refund_datetime,
          any_value(return_fees)           AS return_fees,
          SUM(returned_amount)             AS refunded_gross_incl_vat,
          SUM(returned_tax)                AS refunded_vat,
          SUM(returned_quantity)           AS refunded_units
        FROM orders
        WHERE %s
        GROUP BY order_id
      )
      SELECT
        %s                              AS period,
        SUM(refunded_gross_incl_vat)    AS refunded_gross_incl_vat,
        SUM(refunded_vat)               AS refunded_vat,
        SUM(refunded_units)             AS refunded_units,
        SUM(return_fees)                AS return_fees
      FROM ord
      GROUP BY 1
      ORDER BY 1",
      paste(where_refund, collapse = " AND "),
      bucket("first_refund_datetime")
    )
    refs <- DBI::dbGetQuery(con, q_ref)
    rev <- dplyr::full_join(rev, refs, by = "period")
  }

  num_cols <- setdiff(names(rev), "period")

  out <- rev |>
    dplyr::mutate(dplyr::across(dplyr::all_of(num_cols), ~ dplyr::coalesce(.x, 0))) |>
    dplyr::mutate(
      discount_gap = discounts_true - discounts_line_level_column,

      # --- GetKlar: Original-Bruttowert minus komplette USt.; duties = 0 ---
      net_revenue_getklar = gross_after_discount_incl_vat - vat_total,

      # --- Gegenvariante: zusaetzlich Erstattungen netto abgezogen.
      #     refunded_gross_incl_vat ist inkl. USt. (empirisch:
      #     refunded_vat / refunded_gross_incl_vat = 0,1596-0,1598 ueber alle
      #     Monate 2026 = 19 % auf Brutto-Basis), daher Netto = Brutto - USt.
      net_revenue_getklar_less_refunds =
        net_revenue_getklar - (refunded_gross_incl_vat - refunded_vat)
    ) |>
    dplyr::select(
      period, orders, orders_cancelled, units,
      gross_before_discount_incl_vat, discounts_true,
      discounts_line_level_column, discount_gap,
      shipping_incl_vat, gross_after_discount_incl_vat, vat_total,
      refunded_gross_incl_vat, refunded_vat, refunded_units, return_fees,
      net_revenue_getklar, net_revenue_getklar_less_refunds,
      net_revenue_of_cancelled
    ) |>
    dplyr::arrange(period) |>
    tibble::as_tibble()

  attr(out, "meta") <- list(
    dbdir = db_path,
    date_from = from_iso,
    date_to = to_iso,
    grain = grain,
    refund_basis = refund_basis,
    include_cancelled = include_cancelled,
    getklar_formula = "net_revenue = total_price - total_tax (duties = 0, taxes_included = TRUE)",
    generated_at = Sys.time()
  )

  out
}
