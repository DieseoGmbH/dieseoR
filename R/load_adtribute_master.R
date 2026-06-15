library(arrow)
library(dplyr)
library(lubridate)

#' Initialisiert den virtuellen Master-Datensatz (Lazy Evaluation)
#'
#' @param local_r_path Pfad zur lokalen Konfigurationsdatei. Standard: "~/workspace/local.R"
#' @return Ein virtuelles Arrow-Dataset/Query-Objekt (master_lazy)
load_adtribute_master <- function(local_r_path = "~/workspace/local.R") {
  # 1. Automatisch Pfade über local.R laden
  if (!file.exists(local_r_path)) {
    stop(paste("Kritischer Fehler: Konfigurationsdatei nicht gefunden unter:", local_r_path))
  }

  # Erzeugt eine temporäre Umgebung, um globale Variablen-Konflikte zu vermeiden
  env <- new.env()
  source(local_r_path, local = env)

  if (!exists("datadir", envir = env)) {
    stop("Kritischer Fehler: 'datadir' ist in deiner local.R nicht definiert!")
  }

  datadir_path <- env$datadir
  adtribute_dir <- file.path(datadir_path, "adtribute_parquet_chunks")
  shopify_rds_path <- file.path(datadir_path, "shopify/all_shopify_items_backup.rds")

  # Validierung der Existenz
  if (!dir.exists(adtribute_dir)) stop(paste("Ordner nicht gefunden:", adtribute_dir))
  if (!file.exists(shopify_rds_path)) stop(paste("Shopify-Datei nicht gefunden:", shopify_rds_path))

  # 2. Virtuelles Arrow-Dataset aus Parquet-Chunks initialisieren
  adtribute_ds <- arrow::open_dataset(adtribute_dir, format = "parquet")

  # 3. Shopify laden & als Arrow-Table vorbereiten (RAM-schonend vorfiltriert)
  shopify_arrow <- readRDS(shopify_rds_path) |>
    dplyr::mutate(order_id = as.character(order_id)) |>
    dplyr::select(
      order_id, customer_id, created_at, financial_status,
      total_price, item_gross_revenue, discount_code
    ) |>
    arrow::as_arrow_table()

  # 4. Virtuellen Master-Join & Basis-Kalkulationen vorbereiten (Lazy!)
  master_lazy <- adtribute_ds |>
    dplyr::inner_join(shopify_arrow, by = c("shopify_order_id" = "order_id")) |>
    dplyr::mutate(
      # Umsatz-Splits pro Attributionsmodell
      revenue_last_touch = attribution_weight_last * item_gross_revenue,
      revenue_pammys_opt = weight_pammys_opt * item_gross_revenue,
      revenue_first_touch = attribution_weight_non_direct_first * item_gross_revenue,
      revenue_linear = weight_linear * item_gross_revenue,
      revenue_u_shape = weight_u_shape_30d * item_gross_revenue,

      # Hochperformante C++ Zeitstempel-Transformationen für den Lag
      conv_us = arrow::cast(conversion_attribution_datetime, arrow::int64()),
      touch_us = arrow::cast(touchpoint_attribution_datetime, arrow::int64()),
      lag_days = (conv_us - touch_us) / 86400000000,
      lag_days_rounded = floor(lag_days)
    ) |>
    dplyr::select(-conv_us, -touch_us)

  # Wir geben das LAZY Objekt zurück (Es wurde noch kein Byte im RAM bewegt!)
  return(master_lazy)
}
