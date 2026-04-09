#' @title Bereinigt und entpackt Shopify Bestelldaten (Order-Level zu Item-Level)
#'
#' @description Nimmt rohe Shopify-Bestelldaten, wendet allgemeine Namensbereinigungen
#' an (`clean_master()`), parst Zeitstempel und entpackt die Liste der `line_items`,
#' sodass jede Zeile im Datensatz einem einzelnen gekauften Artikel (Item) entspricht.
#'
#' @param shopify_orders Ein Dataframe oder Tibble mit den rohen Shopify API-Daten (Endpunkt `orders`).
#'
#' @return Ein bereinigtes Tibble auf Item-Ebene.
#' @export
#'
#' @importFrom dplyr mutate filter select across
#' @importFrom tidyr unnest
#' @importFrom purrr map_lgl
#' @importFrom lubridate ymd_hms
#' @importFrom tidyselect ends_with
#'
#' @examples \dontrun{
#' clean_data <- clean_up_shopify(raw_shopify_data)
#' }
clean_up_shopify <- function(shopify_orders) {
  # Sicherheits-Check: Falls ein leerer Chunk uebergeben wird
  if (nrow(shopify_orders) == 0) {
    message("Der uebergebene Shopify-Datensatz ist leer. Gebe leeres Tibble zurueck.")
    return(shopify_orders)
  }

  shopify_item_level <- shopify_orders |>
    clean_master() |>
    # Datumsspalten umwandeln (Namespace-Aufrufe fuer Package-Konformitaet)
    dplyr::mutate(dplyr::across(tidyselect::ends_with("_at"), ~ lubridate::ymd_hms(.))) |>
    # Leere line_items herausfiltern, falls vorhanden
    dplyr::filter(purrr::map_lgl(line_items, ~ length(.x) > 0)) |>
    # Das eigentliche Entpacken.
    # names_sep = "_" sorgt dafuer, dass aus 'sku' -> 'line_items_sku' wird
    tidyr::unnest(cols = c(line_items), names_sep = "_") |>
    # Jetzt waehlen wir die finalen Spalten aus und benennen sie sauber um
    dplyr::select(
      # Order-Metadaten (diese Werte duplizieren sich nun fuer jeden Artikel der Order)
      order_id = id,
      created_at,
      source_name,
      financial_status,
      tags,
      fulfillment_status,
      customer_id,
      shipping_address_country,
      shipping_address_latitude,
      shipping_address_longitude,

      # Item-Metadaten (spezifisch fuer die jeweilige Zeile)
      product_sku = line_items_sku,
      product_title = line_items_title,
      variant_title = line_items_variant_title,
      quantity = line_items_quantity,
      price = line_items_price,
      item_id = line_items_id,
      browser_ip,
      updated_at,
      currency
    ) |>
    # Datentypen bereinigen und erste Item-Metriken berechnen
    dplyr::mutate(
      quantity = as.numeric(quantity),
      price = as.numeric(price),

      # Bruttoumsatz auf Zeilenebene (Einzelpreis * Menge)
      # Das entspricht dem 'gross_revenue' Anteil dieses spezifischen Artikels
      item_gross_revenue = quantity * price,

      # Eine kombinierte Namensspalte bauen (wie bei Adtribute)
      product_title_with_variant = paste(product_title, "-", variant_title)
    )

  return(shopify_item_level)
}
