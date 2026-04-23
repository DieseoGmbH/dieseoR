#' @title Build Daily Business Metrics (P&L + Channel Spend)
#' @description
#' Aggregiert Shopify/COGS-Item-Daten, Retouren/Refunds und Meta-Ads-Spend in eine
#' tägliche Business-Tabelle für das Adtribute-style "Business Overview"-Dashboard
#' (P&L-Kaskade, KPI-Kacheln, Dual-Axis-Plot). Handhabt automatisch drei Fallstricke:
#'
#' \itemize{
#'   \item \strong{Order-Level-Felder} (\code{total_tax}, \code{total_shipping_cost},
#'         \code{total_packaging}, \code{total_payment_fees}) werden über Item-Zeilen
#'         repliziert -- die Funktion dedupliziert pro \code{order_id}, bevor summiert wird,
#'         um Doppelzählung zu verhindern.
#'   \item \strong{Refund-Logik} nutzt \code{order_completed_date} als
#'         cashflow-relevantes Datum und berücksichtigt nur
#'         \code{status == "refunded"} mit \code{type \%in\% c("return", "mix")}.
#'         Reine Exchanges sind revenue-neutral.
#'   \item \strong{NC-Flag (Neukunde)} wird lifetime-basiert berechnet: die erste
#'         chronologische Bestellung je \code{customer_id} im gesamten
#'         \code{financial_data}-Frame.
#' }
#'
#' CM1 und CM2 werden Adtribute-konform berechnet:
#' \code{CM1 = Net Revenue - Product Costs}, \code{CM2 = CM1 - Transaction Costs - Fulfillment Costs},
#' \code{CM3 = CM2 - Channel Spend}.
#'
#' @param financial_data Item-Level-Tibble (z. B. \code{final_financial_data}). Erwartete Spalten:
#'   \code{order_id}, \code{customer_id}, \code{created_at}, \code{cancellation_status},
#'   \code{item_gross_revenue}, \code{item_net_revenue}, \code{total_cogs},
#'   \code{total_shipping_cost}, \code{total_packaging}, \code{total_payment_fees},
#'   \code{quantity}.
#' @param returns_data Refund-Tibble (z. B. \code{returns_cleaned}). Erwartete Spalten:
#'   \code{status}, \code{type}, \code{order_completed_date}, \code{shopify_created_at},
#'   \code{refund_amount}.
#' @param meta_spend_daily Meta-Spend-Tibble (z. B. \code{meta_daily_request}).
#'   Erwartete Spalten: \code{date_start}, \code{spend}, \code{impressions}, \code{clicks}.
#' @param refund_date_basis Einer von \code{"refund_date"} (Default; Refunds auf
#'   \code{order_completed_date} -- matcht Cashflow) oder \code{"conversion_date"}
#'   (Refunds auf ursprüngliches Bestelldatum -- matcht Adtribute's Conversion-Date-Filter
#'   bzw. kohortenbasierte P&L-Sicht).
#'
#' @return Ein \code{tibble} mit einer Zeile pro Tag und den Spalten:
#'   \code{date}, \code{gross_revenue}, \code{taxes}, \code{refunds}, \code{net_revenue},
#'   \code{product_costs}, \code{cm1}, \code{transaction_costs}, \code{fulfillment_costs},
#'   \code{cm2}, \code{channel_spend}, \code{cm3}, \code{channel_impressions},
#'   \code{channel_clicks}, \code{gross_orders}, \code{nc_gross_orders},
#'   \code{nc_gross_revenue}, \code{gross_quantity}.
#'
#' @importFrom dplyr arrange across distinct filter full_join group_by if_else
#'   left_join mutate n_distinct row_number select summarise ungroup
#' @importFrom tidyr replace_na
#' @importFrom rlang .data
#' @export
#'
#' @examples
#' \dontrun{
#' daily_business <- build_daily_business(
#'   financial_data    = final_financial_data,
#'   returns_data      = returns_cleaned,
#'   meta_spend_daily  = meta_daily_request,
#'   refund_date_basis = "refund_date"
#' )
#' }
build_daily_business <- function(financial_data,
                                 returns_data,
                                 meta_spend_daily,
                                 refund_date_basis = c(
                                   "refund_date",
                                   "conversion_date"
                                 )) {
  refund_date_basis <- match.arg(refund_date_basis)

  stopifnot(
    is.data.frame(financial_data),
    is.data.frame(returns_data),
    is.data.frame(meta_spend_daily)
  )

  # --- 1. Lifetime-NC-Flag pro order_id ableiten ---------------------------
  orders_nc <- financial_data |>
    dplyr::filter(!isTRUE(cancellation_status)) |>
    dplyr::distinct(order_id, customer_id, created_at) |>
    dplyr::arrange(customer_id, created_at) |>
    dplyr::group_by(customer_id) |>
    dplyr::mutate(is_nc = dplyr::row_number() == 1L) |>
    dplyr::ungroup() |>
    dplyr::select(order_id, is_nc)

  # --- 2. Item-Level-Aggregate (sum-safe) ---------------------------------
  daily_items <- financial_data |>
    dplyr::filter(!isTRUE(cancellation_status)) |>
    dplyr::mutate(date = as.Date(created_at)) |>
    dplyr::left_join(orders_nc, by = "order_id") |>
    dplyr::mutate(is_nc = tidyr::replace_na(is_nc, FALSE)) |>
    dplyr::group_by(date) |>
    dplyr::summarise(
      gross_revenue = sum(item_gross_revenue, na.rm = TRUE),
      net_revenue_items = sum(item_net_revenue, na.rm = TRUE),
      product_costs = sum(total_cogs, na.rm = TRUE),
      gross_quantity = sum(quantity, na.rm = TRUE),
      nc_gross_revenue = sum(item_gross_revenue[is_nc],
        na.rm = TRUE
      ),
      .groups = "drop"
    ) |>
    # Taxes aus Gross/Net ableiten -> kein Dedup-Problem auf Order-Ebene noetig
    dplyr::mutate(taxes = gross_revenue - net_revenue_items)

  # --- 3. Order-Level-Aggregate (order_id-dedupliziert) --------------------
  daily_orders <- financial_data |>
    dplyr::filter(!isTRUE(cancellation_status)) |>
    dplyr::mutate(date = as.Date(created_at)) |>
    dplyr::left_join(orders_nc, by = "order_id") |>
    dplyr::mutate(is_nc = tidyr::replace_na(is_nc, FALSE)) |>
    dplyr::distinct(
      order_id, date, is_nc,
      total_shipping_cost, total_packaging, total_payment_fees
    ) |>
    dplyr::group_by(date) |>
    dplyr::summarise(
      fulfillment_costs = sum(total_shipping_cost, na.rm = TRUE) +
        sum(total_packaging, na.rm = TRUE),
      transaction_costs = sum(total_payment_fees, na.rm = TRUE),
      gross_orders = dplyr::n_distinct(order_id),
      nc_gross_orders = sum(is_nc, na.rm = TRUE),
      .groups = "drop"
    )

  # --- 4. Refunds pro Tag -------------------------------------------------
  refund_date_col <- if (refund_date_basis == "refund_date") {
    "order_completed_date"
  } else {
    "shopify_created_at"
  }

  daily_refunds <- returns_data |>
    dplyr::filter(
      status == "refunded",
      type %in% c("return", "mix")
    ) |>
    dplyr::mutate(date = as.Date(.data[[refund_date_col]])) |>
    dplyr::filter(!is.na(date)) |>
    dplyr::group_by(date) |>
    dplyr::summarise(
      refunds = sum(refund_amount, na.rm = TRUE),
      .groups = "drop"
    )

  # --- 5. Meta-Ads-Spend pro Tag ------------------------------------------
  daily_spend <- meta_spend_daily |>
    dplyr::mutate(date = as.Date(date_start)) |>
    dplyr::group_by(date) |>
    dplyr::summarise(
      channel_spend = sum(spend, na.rm = TRUE),
      channel_impressions = sum(impressions, na.rm = TRUE),
      channel_clicks = sum(clicks, na.rm = TRUE),
      .groups = "drop"
    )

  # --- 6. Zusammenfuehren & CM-Kaskade ------------------------------------
  daily_items |>
    dplyr::full_join(daily_orders, by = "date") |>
    dplyr::full_join(daily_refunds, by = "date") |>
    dplyr::full_join(daily_spend, by = "date") |>
    dplyr::mutate(
      dplyr::across(-date, ~ tidyr::replace_na(.x, 0))
    ) |>
    dplyr::mutate(
      net_revenue = gross_revenue - taxes - refunds,
      cm1         = net_revenue - product_costs,
      cm2         = cm1 - transaction_costs - fulfillment_costs,
      cm3         = cm2 - channel_spend
    ) |>
    dplyr::select(
      date,
      gross_revenue, taxes, refunds, net_revenue,
      product_costs, cm1,
      transaction_costs, fulfillment_costs, cm2,
      channel_spend, cm3,
      channel_impressions, channel_clicks,
      gross_orders, nc_gross_orders, nc_gross_revenue,
      gross_quantity
    ) |>
    dplyr::arrange(date)
}
