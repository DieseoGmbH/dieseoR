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
#' @importFrom dplyr mutate filter select across any_of
#' @importFrom tidyr unnest
#' @importFrom purrr map_lgl
#' @importFrom lubridate ymd_hms
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
      shopify_data |>
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

          # 4. Cancellation Status
          cancellation_status = !is.na(cancelled_at)
        ) |>
        dplyr::filter(purrr::map_lgl(line_items, ~ length(.x) > 0)) |>
        tidyr::unnest(cols = c(line_items), names_sep = "_") |>
        dplyr::select(
          # --- Standard & IDs ---
          order_id = id,
          shopify_order_name = name, # z.B. #121923300
          identity = email, # für Adtribute Match
          created_at,
          sales_channel = source_name,
          financial_status,
          tags,
          fulfillment_status,
          customer_id,

          # --- Adtribute Extraktionen ---
          payment_method,
          discount_code,
          cancellation_status,
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
          price = line_items_price,
          item_id = line_items_id,

          # --- Location & System ---
          shipping_address_country,
          shipping_address_latitude,
          shipping_address_longitude,
          browser_ip,
          updated_at,
          currency
        ) |>
        dplyr::mutate(
          quantity = as.numeric(quantity),
          price = as.numeric(price),
          total_price = as.numeric(total_price),
          total_discounts = as.numeric(total_discounts),
          total_tax = as.numeric(total_tax),
          item_gross_revenue = quantity * price,
          product_title_with_variant = paste(product_title, "-", variant_title)
        )
    },

    # ---------------------------------------------------------
    # 2. CHECKOUTS
    # ---------------------------------------------------------
    "checkouts" = {
      shopify_data |>
        clean_master() |>
        dplyr::mutate(dplyr::across(tidyselect::ends_with("_at"), ~ lubridate::ymd_hms(.))) |>
        dplyr::filter(purrr::map_lgl(line_items, ~ length(.x) > 0)) |>
        tidyr::unnest(cols = c(line_items), names_sep = "_") |>
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
