#' @title Bereinigt und entpackt Shopify Daten dynamisch (Orders, Checkouts, Products, Customers)
#'
#' @description Nimmt rohe Shopify-Daten und wendet je nach Endpunkt spezifische
#' Bereinigungen an. Nutzt `clean_master()` fuer Namenskonventionen und entpackt Listen
#' wie `line_items` oder `variants`.
#'
#' @param shopify_data Ein Dataframe oder Tibble mit den rohen Shopify API-Daten.
#' @param endpoint Character. Der Name des Endpunkts ("orders", "checkouts", "products", "customers").
#'
#' @return Ein bereinigtes Tibble, passend zum Endpunkt.
#' @export
#'
#' @importFrom dplyr mutate filter select across any_of left_join group_by summarise coalesce first ungroup bind_rows na_if if_else
#' @importFrom tidyr unnest
#' @importFrom purrr map_chr map_lgl
#' @importFrom lubridate ymd_hms
#' @importFrom stringr str_to_title
#' @importFrom tidyselect ends_with starts_with
clean_up_shopify <- function(shopify_data, endpoint = "orders") {
  # Sicherheits-Check: Falls ein leerer Chunk uebergeben wird
  if (nrow(shopify_data) == 0) {
    message("Der uebergebene Shopify-Datensatz ist leer. Gebe leeres Tibble zurueck.")
    return(shopify_data)
  }

  # Dynamische Bereinigung je nach Endpunkt
  cleaned_data <- switch(endpoint,

    # ---------------------------------------------------------
    # 1. ORDERS
    # ---------------------------------------------------------
    "orders" = {
      # 🔥 BUGFIX: Spalte 'cancelled_at' initialisieren, falls Shopify sie weglässt
      if (!"cancelled_at" %in% names(shopify_data)) {
        shopify_data$cancelled_at <- NA_character_
      }

      # Basis: Namen standardisieren + Order-Level-Felder aus Listen entpacken
      base <- shopify_data |>
        clean_master() |>
        dplyr::mutate(
          dplyr::across(tidyselect::ends_with("_at"), ~ lubridate::ymd_hms(.)),

          # 1. Payment Method entpacken
          payment_method = purrr::map_chr(payment_gateway_names, ~ paste(.x, collapse = ", ")),

          # 2. Discount Codes entpacken
          discount_code = purrr::map_chr(discount_codes, function(x) {
            if (length(x) > 0 && "code" %in% names(x)) paste(x$code, collapse = ", ") else NA_character_
          }),

          # 3. First Refund Date entpacken
          first_refund_datetime = purrr::map_chr(refunds, function(x) {
            if (length(x) > 0 && "created_at" %in% names(x)) {
              as.character(min(lubridate::ymd_hms(x$created_at), na.rm = TRUE))
            } else {
              NA_character_
            }
          }),
          first_refund_datetime = lubridate::ymd_hms(first_refund_datetime),

          # 4. Fulfillment Date entpacken
          fulfillment_date = purrr::map_chr(fulfillments, function(x) {
            if (length(x) > 0 && "created_at" %in% names(x)) {
              as.character(min(lubridate::ymd_hms(x$created_at), na.rm = TRUE))
            } else {
              NA_character_
            }
          }),
          fulfillment_date = lubridate::ymd_hms(fulfillment_date),

          # 5. Cancellation Status (Funktioniert jetzt immer durch den Bugfix)
          cancellation_status = !is.na(cancelled_at)
        )

      # 🔧 Robustheit: Order-Level-Rohspalten sicherstellen (Shopify kann sie weglassen)
      for (.col in c(
        "total_shipping_price_set_shop_money_amount", "shipping_address_country_code",
        "customer_first_name", "customer_last_name",
        "billing_address_first_name", "billing_address_last_name",
        "shipping_address_first_name", "shipping_address_last_name"
      )) {
        if (!.col %in% names(base)) base[[.col]] <- NA_character_
      }

      # 👤 Kundenname (Order-Level). clean_master() hat alle Textspalten ge-lowercased
      # -> fuer die Anzeige title-case. Quelle-Prioritaet: customer -> billing -> shipping.
      base <- base |>
        dplyr::mutate(
          first_name = stringr::str_to_title(dplyr::coalesce(
            dplyr::na_if(customer_first_name, ""),
            dplyr::na_if(billing_address_first_name, ""),
            dplyr::na_if(shipping_address_first_name, "")
          )),
          last_name = stringr::str_to_title(dplyr::coalesce(
            dplyr::na_if(customer_last_name, ""),
            dplyr::na_if(billing_address_last_name, ""),
            dplyr::na_if(shipping_address_last_name, "")
          ))
        )

      # 🧾 Order-Level-Steuer aus den `tax_lines` ziehen.
      #    Grund: Bei TikTok-Shop-Bestellungen liefert Shopify `total_tax = 0`,
      #    obwohl die Steuer in `tax_lines` (und `current_total_tax`) sehr wohl
      #    ausgewiesen ist. Seit April 2026 betrifft das 6-10 % aller Orders.
      #    `total_tax` bleibt unveraendert (Roh-API-Wert), `total_tax_effective`
      #    ist der belastbare Wert fuer jede Steuerrechnung.
      base <- base |>
        dplyr::mutate(
          order_tax_lines_total = .sum_nested(tax_lines, "price"),
          tax_rates_n           = .count_nested_distinct(tax_lines, "rate"),
          tax_rate_primary      = .top_nested_by(tax_lines, "rate", "price")
        )

      # 💶 Refund-Aggregate aus den verschachtelten `refunds` ziehen:
      #    - je line_item_id: erstatteter Produktwert / Steuer / Menge (Line-Item-Grain)
      #    - je order_id:     Return-Fees, Versand-Erstattungen und die
      #                       vollstaendige order_adjustments-Summe (Order-Grain)
      refund_agg <- .shopify_refund_aggregates(base$refunds, base$id)

      # ⚡ SPEICHER: Verschachtelte Order-Spalten VOR dem unnest wegwerfen.
      #    unnest() vervielfacht jede Order-Zeile auf ~2,4 Positionszeilen und
      #    kopiert dabei auch die grossen Listenspalten mit. Alles, was daraus
      #    gebraucht wird, ist oben bereits aggregiert (payment_method,
      #    discount_code, first_refund_datetime, fulfillment_date,
      #    order_tax_lines_total, refund_agg). Ohne diesen Schritt braucht ein
      #    Delta von 200k Orders >12 GB Zwischenspeicher und die Maschine swappt.
      .drop_nested <- c(
        "refunds", "fulfillments", "discount_applications", "discount_codes",
        "payment_gateway_names", "note_attributes", "tax_lines", "shipping_lines",
        "refunds_transactions", "shipping_lines_tax_lines"
      )
      base <- base |> dplyr::select(-dplyr::any_of(.drop_nested))
      gc(verbose = FALSE)

      flat <- base |>
        tidyr::unnest(cols = c(line_items), names_sep = "_", keep_empty = TRUE)
      rm(base)
      gc(verbose = FALSE)

      # 🏷️ Positions-Rabatte VOLLSTAENDIG erfassen.
      #    `line_items.total_discount` enthaelt nur Rabatte auf Positionsebene.
      #    Order-Level-Rabattcodes verteilt Shopify ueber `discount_allocations`;
      #    ohne sie fehlen 76-89 % des Rabattvolumens.
      #    Robustheit: Shopify laesst die Listenspalten bei leeren Chunks weg.
      for (.lc in c("line_items_discount_allocations", "line_items_tax_lines")) {
        if (!.lc %in% names(flat)) flat[[.lc]] <- vector("list", nrow(flat))
      }

      # 🔧 Robustheit: Enthaelt ein Chunk ausschliesslich Orders ohne Line Items,
      #    legt unnest() keine line_items_*-Spalten an und das select() unten
      #    bricht ab. Fehlende Spalten typrichtig vorbelegen.
      for (.lc in c(
        "line_items_id", "line_items_quantity", "line_items_current_quantity",
        "line_items_price", "line_items_product_id", "line_items_variant_id",
        "line_items_total_discount"
      )) {
        if (!.lc %in% names(flat)) flat[[.lc]] <- NA_real_
      }
      for (.lc in c(
        "line_items_sku", "line_items_title", "line_items_variant_title",
        "line_items_fulfillment_status"
      )) {
        if (!.lc %in% names(flat)) flat[[.lc]] <- NA_character_
      }

      flat <- flat |>
        dplyr::mutate(
          discount_amount_allocated = .sum_nested(line_items_discount_allocations, "amount"),
          line_item_tax             = .sum_nested(line_items_tax_lines, "price")
        )

      flat |>
        dplyr::select(
          # --- Standard & IDs ---
          order_id = id,
          shopify_order_name = name,
          identity = email,
          first_name,
          last_name,
          created_at,
          fulfillment_date,
          sales_channel = source_name,
          financial_status,
          tags,
          fulfillment_status,
          customer_id,
          note,

          # --- Adtribute Extraktionen ---
          payment_method,
          discount_code,
          cancellation_status,
          cancelled_at,
          first_refund_datetime,

          # --- Adtribute Financials (Order Level) ---
          total_price,
          total_discounts,
          total_tax,

          # --- Line Items ---
          product_sku = line_items_sku,
          product_title = line_items_title,
          variant_title = line_items_variant_title,
          quantity = line_items_quantity,
          current_quantity = line_items_current_quantity,
          price = line_items_price,
          item_id = line_items_id,
          line_items_fulfillment_status,
          line_items_total_discount,
          line_items_product_id,
          line_items_variant_id,

          # --- Location & System ---
          shipping_address_country,
          shipping_address_country_code,
          shipping_address_latitude,
          shipping_address_longitude,
          browser_ip,
          updated_at,
          currency,
          landing_site,
          referring_site,
          landing_site_ref,

          # --- NEU: Versandumsatz (Order Level, roh) ---
          shipping_charges = total_shipping_price_set_shop_money_amount,

          # --- NEU: Steuer- und Rabatt-Korrekturen (siehe Kommentare oben) ---
          order_tax_lines_total,
          tax_rates_n,
          tax_rate_primary,
          discount_amount_allocated,
          line_item_tax
        ) |>
        # Refund-Aggregate anfuegen (Line-Item- bzw. Order-Grain)
        dplyr::left_join(refund_agg$item, by = c("item_id" = "line_item_id")) |>
        dplyr::left_join(refund_agg$order, by = "order_id") |>
        dplyr::mutate(
          quantity = as.numeric(quantity),
          price = as.numeric(price),
          total_price = as.numeric(total_price),
          total_discounts = as.numeric(total_discounts),
          total_tax = as.numeric(total_tax),
          item_gross_revenue = quantity * price,
          product_title_with_variant = paste(product_title, "-", variant_title),

          # ==== Sales-Report-Kennzahlen nach Shopify-Definition (aus Rohdaten) ====
          # -- Line-Item-Grain (pro Zeile eindeutig -> summierbar) --
          gross_sales = item_gross_revenue,
          discount_amount = dplyr::coalesce(suppressWarnings(as.numeric(line_items_total_discount)), 0),
          returned_amount = dplyr::coalesce(returned_amount, 0),
          returned_tax = dplyr::coalesce(returned_tax, 0),
          returned_quantity = dplyr::coalesce(returned_quantity, 0),
          net_sales = gross_sales - discount_amount - returned_amount,

          # -- Order-Grain (pro Order gleich, ueber die Item-Zeilen wiederholt) --
          shipping_charges = dplyr::coalesce(suppressWarnings(as.numeric(shipping_charges)), 0),
          return_fees = dplyr::coalesce(return_fees, 0),
          shipping_address_country_code = toupper(shipping_address_country_code),

          # ==== KORREKTUR-SPALTEN (additiv!) ====
          # Bestehende Spalten behalten bewusst ihre Semantik. Die Delta-Pipeline
          # berueht nur geaenderte Orders; wuerde man `discount_amount` oder
          # `total_tax` in-place korrigieren, haette dieselbe Spalte je nach
          # letztem Update-Zeitpunkt zwei verschiedene Bedeutungen. Neue Spalten
          # sind bis zum Backfill NULL — das ist erkennbar, eine gemischte
          # Semantik waere es nicht.

          # -- Steuer: tax_lines schlagen total_tax, wenn dieses 0 ist (TikTok) --
          order_tax_lines_total = dplyr::coalesce(order_tax_lines_total, 0),
          total_tax_effective = dplyr::if_else(
            dplyr::coalesce(total_tax, 0) == 0 & order_tax_lines_total > 0,
            order_tax_lines_total,
            dplyr::coalesce(total_tax, 0)
          ),

          # -- Refund-Bestandteile fuer die Portal-Abstimmung --
          refund_adjustments_total = dplyr::coalesce(refund_adjustments_total, 0),
          refund_shipping_total = dplyr::coalesce(refund_shipping_total, 0),

          # -- Korrigierte Sales-Kennzahlen auf Basis der vollstaendigen Rabatte --
          net_sales_corrected = gross_sales -
            dplyr::coalesce(discount_amount_allocated, discount_amount) - returned_amount
        ) |>
        dplyr::group_by(order_id) |>
        dplyr::mutate(
          # Netto-Steuer der Order = berechnete Steuer - erstattete Steuer
          net_tax = dplyr::coalesce(dplyr::first(total_tax), 0) - sum(returned_tax, na.rm = TRUE),
          # Total sales (Shopify) = Netto-Umsatz + Netto-Steuer + Versand + Return-Fees
          total_sales = sum(net_sales, na.rm = TRUE) +
            net_tax +
            dplyr::first(shipping_charges) +
            dplyr::first(return_fees),

          # Korrigierte Varianten (vollstaendige Rabatte + tax_lines-Steuer)
          net_tax_effective = dplyr::coalesce(dplyr::first(total_tax_effective), 0) -
            sum(returned_tax, na.rm = TRUE),
          total_sales_corrected = sum(net_sales_corrected, na.rm = TRUE) +
            net_tax_effective +
            dplyr::first(shipping_charges) +
            dplyr::first(return_fees)
        ) |>
        dplyr::ungroup()
    },

    # ---------------------------------------------------------
    # 2. CHECKOUTS
    # ---------------------------------------------------------
    "checkouts" = {
      shopify_data |>
        clean_master() |>
        dplyr::mutate(dplyr::across(tidyselect::ends_with("_at"), ~ lubridate::ymd_hms(.))) |>
        tidyr::unnest(cols = c(line_items), names_sep = "_", keep_empty = TRUE) |>
        dplyr::select(
          order_id = id, created_at, source_name, customer_id,
          shipping_address_country, shipping_address_latitude, shipping_address_longitude,
          product_sku = line_items_sku, product_title = line_items_title,
          variant_title = line_items_variant_title, quantity = line_items_quantity,
          price = line_items_price, item_id = line_items_product_id, updated_at,
          currency, buyer_accepts_marketing, total_discounts, total_tax, total_weight
        ) |>
        dplyr::mutate(
          quantity = as.numeric(quantity),
          price = as.numeric(price),
          total_weight = as.numeric(total_weight),
          total_tax = as.numeric(total_tax),
          total_discounts = as.numeric(total_discounts),
          item_gross_revenue = quantity * price,
          product_title_with_variant = paste(product_title, "-", variant_title)
        )
    },

    # ---------------------------------------------------------
    # 3. PRODUCTS
    # ---------------------------------------------------------
    "products" = {
      shopify_data |>
        tidyr::unnest(c(variants), names_sep = "_") |>
        clean_master() |>
        dplyr::select(-c(
          dplyr::any_of(c(
            "published_scope", "body_html", "admin_graphql_api_id",
            "variants_compare_at_price", "variants_product_id",
            "variants_fulfillment_service", "variants_barcode",
            "variants_position", "variants_inventory_management",
            "variants_weight", "variants_weight_unit", "variants_image_id",
            "options", "variants_admin_graphql_api_id",
            "variants_inventory_item_id", "variants_option3", "tags",
            "template_suffix", "variants_old_inventory_quantity"
          )),
          tidyselect::starts_with("image")
        )) |>
        dplyr::mutate(
          dplyr::across(tidyselect::ends_with("_at"), ~ lubridate::ymd_hms(.)),
          variants_price = as.numeric(variants_price),
          variants_inventory_quantity = as.numeric(variants_inventory_quantity)
        )
    },

    # ---------------------------------------------------------
    # 4. CUSTOMERS
    # ---------------------------------------------------------
    "customers" = {
      shopify_data |>
        dplyr::mutate(
          dplyr::across(tidyselect::ends_with("_at"), ~ lubridate::ymd_hms(.)),
          total_spent = as.numeric(total_spent)
        ) |>
        dplyr::select(-c(
          dplyr::any_of(c(
            "tax_exemptions", "note", "admin_graphql_api_id",
            "first_name", "last_name", "last_order_id", "email",
            "phone", "addresses"
          )),
          tidyselect::starts_with("default_")
        )) |>
        clean_master() |>
        dplyr::select(-dplyr::any_of(c(
          "sms_marketing_consent_consent_updated_at",
          "sms_marketing_consent_consent_collected_from", "sms_marketing_consent",
          "multipass_identifier", "email_marketing_consent_consent_updated_at"
        )))
    },

    # ---------------------------------------------------------
    # FALLBACK
    # ---------------------------------------------------------
    {
      stop(sprintf("Fehler: Endpunkt '%s' wird in clean_up_shopify() noch nicht unterstuetzt.", endpoint), call. = FALSE)
    }
  )

  return(cleaned_data)
}

# ---------------------------------------------------------------------------
# Interne Helper (nicht exportiert) fuer verschachtelte Shopify-Strukturen.
# Alle arbeiten auf einer Liste von Data.Frames (eine Liste je Zeile) und geben
# einen Vektor derselben Laenge zurueck -> direkt in dplyr::mutate() nutzbar.
# ---------------------------------------------------------------------------

# Summe eines numerischen Feldes ueber die verschachtelten Data.Frames.
.sum_nested <- function(lst, feld) {
  if (is.null(lst)) {
    return(numeric(0))
  }
  vapply(lst, function(x) {
    if (is.data.frame(x) && nrow(x) > 0 && feld %in% names(x)) {
      sum(suppressWarnings(as.numeric(x[[feld]])), na.rm = TRUE)
    } else {
      0
    }
  }, numeric(1))
}

# Anzahl verschiedener Werte eines Feldes (z. B. wie viele Steuersaetze je Order).
.count_nested_distinct <- function(lst, feld) {
  if (is.null(lst)) {
    return(integer(0))
  }
  vapply(lst, function(x) {
    if (is.data.frame(x) && nrow(x) > 0 && feld %in% names(x)) {
      v <- suppressWarnings(as.numeric(x[[feld]]))
      length(unique(v[!is.na(v)]))
    } else {
      0L
    }
  }, integer(1))
}

# Wert von `feld` in der Zeile mit dem groessten `gewicht` (z. B. dominanter Steuersatz).
.top_nested_by <- function(lst, feld, gewicht) {
  if (is.null(lst)) {
    return(numeric(0))
  }
  vapply(lst, function(x) {
    if (is.data.frame(x) && nrow(x) > 0 && all(c(feld, gewicht) %in% names(x))) {
      v <- suppressWarnings(as.numeric(x[[feld]]))
      w <- suppressWarnings(as.numeric(x[[gewicht]]))
      if (all(is.na(w))) {
        return(NA_real_)
      }
      v[which.max(replace(w, is.na(w), -Inf))]
    } else {
      NA_real_
    }
  }, numeric(1))
}

# ---------------------------------------------------------------------------
# Interner Helper (nicht exportiert): Refund-Aggregate aus verschachtelten
# Shopify-`refunds`. Liefert zwei Data.Frames:
#   $item : line_item_id, returned_amount, returned_tax, returned_quantity
#           -> Line-Item-Grain, ueber Teil-Refunds hinweg pro line_item_id summiert.
#   $order: order_id, return_fees
#           -> Order-Grain; Return-/Restocking-Fees aus order_adjustments
#              (Best-Effort: kind != 'shipping_refund'; in den aktuellen Daten meist leer).
# Betraege werden als positive Magnituden zurueckgegeben (net_sales = gross - disc - returns).
.shopify_refund_aggregates <- function(refunds_list, order_ids) {
  li_rows <- list()
  oa_rows <- list()

  for (i in seq_along(refunds_list)) {
    r <- refunds_list[[i]]
    if (is.null(r) || !is.data.frame(r) || nrow(r) == 0) next
    oid <- order_ids[[i]]

    # -- refund_line_items: erstatteter Produktwert / Steuer / Menge je Line-Item --
    if ("refund_line_items" %in% names(r)) {
      for (j in seq_len(nrow(r))) {
        rli <- r$refund_line_items[[j]]
        if (is.data.frame(rli) && nrow(rli) > 0 && "line_item_id" %in% names(rli)) {
          li_rows[[length(li_rows) + 1L]] <- data.frame(
            line_item_id      = as.numeric(rli$line_item_id),
            returned_amount   = if ("subtotal" %in% names(rli)) as.numeric(rli$subtotal) else NA_real_,
            returned_tax      = if ("total_tax" %in% names(rli)) as.numeric(rli$total_tax) else NA_real_,
            returned_quantity = if ("quantity" %in% names(rli)) as.numeric(rli$quantity) else NA_real_,
            stringsAsFactors  = FALSE
          )
        }
      }
    }

    # -- order_adjustments: Return-Fees, Versand-Erstattungen, Gesamtsumme --
    #    Zwei Kinds kommen in den Pammys-Daten vor:
    #      refund_discrepancy : positiver Betrag, korrigiert einen Refund nach unten
    #                           (u. a. fehlgeschlagene Viva-Erstattungen)
    #      shipping_refund    : negativer Betrag, erhoeht den Refund um den Versand
    #    Adtribute rechnet Refund = SUM(refund_line_items.subtotal) - SUM(adjustments.amount).
    #    `refund_adjustments_total` haelt die Rohsumme, damit das reproduzierbar ist.
    if ("order_adjustments" %in% names(r)) {
      for (j in seq_len(nrow(r))) {
        oa <- r$order_adjustments[[j]]
        if (is.data.frame(oa) && nrow(oa) > 0 && "amount" %in% names(oa)) {
          kind <- if ("kind" %in% names(oa)) as.character(oa$kind) else rep(NA_character_, nrow(oa))
          amt <- suppressWarnings(as.numeric(oa$amount))
          # Fee = einbehaltener Betrag (negativer adjustment amount), ohne reine Versand-Refunds
          fee <- sum(-amt[!is.na(kind) & kind != "shipping_refund"], na.rm = TRUE)
          oa_rows[[length(oa_rows) + 1L]] <- data.frame(
            order_id = oid,
            return_fees = fee,
            refund_adjustments_total = sum(amt, na.rm = TRUE),
            refund_shipping_total = sum(amt[!is.na(kind) & kind == "shipping_refund"], na.rm = TRUE),
            stringsAsFactors = FALSE
          )
        }
      }
    }
  }

  item_agg <- if (length(li_rows) > 0) {
    dplyr::bind_rows(li_rows) |>
      dplyr::group_by(line_item_id) |>
      dplyr::summarise(
        returned_amount = sum(returned_amount, na.rm = TRUE),
        returned_tax = sum(returned_tax, na.rm = TRUE),
        returned_quantity = sum(returned_quantity, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    data.frame(
      line_item_id = numeric(0), returned_amount = numeric(0),
      returned_tax = numeric(0), returned_quantity = numeric(0)
    )
  }

  order_agg <- if (length(oa_rows) > 0) {
    dplyr::bind_rows(oa_rows) |>
      dplyr::group_by(order_id) |>
      dplyr::summarise(
        return_fees = sum(return_fees, na.rm = TRUE),
        refund_adjustments_total = sum(refund_adjustments_total, na.rm = TRUE),
        refund_shipping_total = sum(refund_shipping_total, na.rm = TRUE),
        .groups = "drop"
      )
  } else {
    data.frame(
      order_id = numeric(0), return_fees = numeric(0),
      refund_adjustments_total = numeric(0), refund_shipping_total = numeric(0)
    )
  }

  list(item = item_agg, order = order_agg)
}
