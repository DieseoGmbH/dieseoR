# ---------------------------------------------------------------------------
# Shopify-Arbeitsdatensatz: schlanker Auszug der Master-DuckDB als RDS
#
# Zweck: eine Datei, die sich direkt wie ein tibble benutzen laesst --
#        readRDS("~/data/shopify/shopify_data.rds")
#        ohne DuckDB-Verbindung, ohne Lock-Risiko fuer die Nacht-Pipeline.
#
# Manuell aufrufen, wenn ein frischer Stand gebraucht wird:
#        Rscript ~/git/dieseoR/scripts/produktiv/export_shopify_working_set.R
#
# ---------------------------------------------------------------------------
# WARUM ES DIESE DATEI GIBT
# Die Master-DuckDB hat 68 Spalten und 6,15 Mio. Zeilen. Wer sie offen haelt,
# blockiert den Blue-Green-Swap des Nachtlaufs -- update_shopify_data() meldet
# dann trotzdem "erfolgreich" (der Fehlschlag steckt nur im Rueckgabewert) und
# das Dashboard zeigt am naechsten Morgen still veraltete Zahlen.
# Dieses Skript oeffnet die DB read-only, zieht den Auszug und schliesst sofort.
#
# SPALTENAUSWAHL unten anpassbar. Bewusst weggelassen: Geokoordinaten,
# browser_ip, landing_site*/referring_site, line_items_*, die Steuer-Detail-
# und Korrekturspalten. Wer die braucht, geht an den Master.
# ---------------------------------------------------------------------------

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
  library(dplyr)
})

SHOPIFY_DB <- "~/data/shopify/shopify.duckdb"
OUT_RDS <- "~/data/shopify/shopify_data.rds"
MIN_DATE <- NULL # z. B. "2025-01-01" begrenzt auf juengere Bestellungen; NULL = alles

SPALTEN <- c(
  # Identitaet & Zeit
  "order_id", "shopify_order_name", "item_id", "created_at", "updated_at",
  "fulfillment_date", "cancelled_at",
  # Status
  "financial_status", "fulfillment_status", "cancellation_status",
  # Kunde
  "customer_id", "identity", "first_name", "last_name",
  # Bestellung (Order-Ebene, ueber die Positionen konstant)
  "total_price", "total_discounts", "total_tax",
  # Position (Item-Ebene)
  "product_sku", "product_title", "variant_title", "product_title_with_variant",
  "quantity", "current_quantity", "price", "item_gross_revenue",
  # Kanal, Zahlung, Marketing
  "payment_method", "sales_channel", "discount_code", "tags",
  # Land
  "shipping_address_country_code",
  # Umsatz-/Retouren-Kennzahlen aus dem Master
  "gross_sales", "discount_amount", "net_sales", "total_sales",
  "returned_amount", "returned_quantity"
)

con <- dbConnect(duckdb::duckdb(), dbdir = path.expand(SHOPIFY_DB), read_only = TRUE)
on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

# Schema-Evolution-robust: nur nehmen, was es auch wirklich gibt (gleiches
# Muster wie der Data-Mart-Bau in update_dashboard.R).
vorhanden <- dbListFields(con, "orders")
fehlend <- setdiff(SPALTEN, vorhanden)
if (length(fehlend)) {
  warning(
    "Nicht im Master, wird uebersprungen: ",
    paste(fehlend, collapse = ", ")
  )
}
nehmen <- intersect(SPALTEN, vorhanden)

message("Ziehe ", length(nehmen), " von ", length(vorhanden), " Spalten ...")

q <- dplyr::tbl(con, "orders") |> dplyr::select(dplyr::all_of(nehmen))
if (!is.null(MIN_DATE)) {
  grenze <- as.POSIXct(MIN_DATE, tz = "")
  q <- q |> dplyr::filter(created_at >= grenze)
}

shopify_data <- dplyr::as_tibble(dplyr::collect(q))

message(
  "Geladen: ", format(nrow(shopify_data), big.mark = ".", decimal.mark = ","),
  " Zeilen x ", ncol(shopify_data), " Spalten | ",
  round(as.numeric(utils::object.size(shopify_data)) / 1024^3, 2), " GB im RAM"
)

out <- path.expand(OUT_RDS)
# gzip statt xz: gemessen 41 s statt ~6 min beim Schreiben und 14 s statt 23 s
# beim Lesen, Preis sind 77 MB mehr auf der Platte (230 statt 153 MB). Fuer eine
# Datei, die regelmaessig neu gebaut wird, ist das der klar bessere Tausch.
saveRDS(shopify_data, out, compress = TRUE)

message(
  "Gespeichert: ", out, " (",
  round(file.size(out) / 1024^2, 1), " MB auf Platte)"
)
message("Nutzung:  shopify_data <- readRDS(\"", OUT_RDS, "\")")
