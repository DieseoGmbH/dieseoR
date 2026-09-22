# -------------------------------------------------------------------------
# Skript: ~/git/dieseoR/scripts/produktiv/get_adtribute_spend_data_parquet.R
# Beschreibung: Zieht aggregierte Werbekosten iterativ als .parquet.
# Mit Smart Backoff (dynamische page_size) gegen BigQuery API-Timeouts.
#
# UMSTELLUNG 2026-07-09: Quelle ist die NEUE virtuelle View
#   rep_attribution_conversion_cmo172fip000s91ja3l61wqum
# (Adtribute hat im Mai auf View-basiertes Data-Sharing migriert; die alte
#  physische Tabelle rep_attribution_conversion_full_daily ist seit 18.05.
#  EINGEFROREN und lieferte zudem 2-8 % zu wenig Spend — Restatements fehlten.)
# Die neue View ist STÜNDLICH granular (~1 Mio. Zeilen/Tag) -> wir aggregieren
# in BigQuery auf die bisherige Chunk-Granularität (Tag x Channel x Kampagne x
# Adset x Ad); Spaltennamen/Schema bleiben identisch, alle Konsumenten
# (data_prep.R) rechnen ohnehin nur SUM() darauf.
# Zukunfts-Zeilen (geplante/gebuchte Kosten bis 2027) werden auf heute gecappt.
# -------------------------------------------------------------------------

if (file.exists("~/workspace/local.R")) {
  source("~/workspace/local.R")
} else {
  stop("local.R nicht gefunden! Bitte Pfad prüfen.")
}

library(bigrquery)
library(dplyr)
library(arrow)
library(lubridate)

# 1. Konfiguration und Authentifizierung
json_key_path <- "~/git/dieseoR/scripts/auth_keys/pammys-analytics-bac507b00184.json"
tryCatch(
  {
    bigrquery::bq_auth(path = json_key_path)
    message("✅ Erfolgreich bei Google BigQuery authentifiziert.")
  },
  error = function(e) stop("Auth Fehler: ", e$message)
)

project_id <- "pammys-analytics"
dataset_name <- "adtribute_raw"
tbl_name <- "rep_attribution_conversion_cmo172fip000s91ja3l61wqum"

data_dir <- file.path(datadir, "adtribute_spend_chunks")
if (!dir.exists(data_dir)) dir.create(data_dir, recursive = TRUE)

# 2. FULL-REFRESH in EINEM Query: Die vor-aggregierten Daten sind klein
# (~2 Mio. Zeilen gesamt), und Adtribute bucht Spend auch Monate rückwirkend
# nach (Restatements: Jan 2026 bekam z.B. +5 % nach 6 Monaten). Deshalb jede
# Nacht die komplette Historie neu ziehen (1 Scan der View ≈ Centbetrag) und
# lokal in Monats-Chunks splitten — atomarer Austausch je Datei.
start_date <- as.Date("2024-11-01")

query <- sprintf(
  "SELECT
    date,
    channel_name,
    channel_campaign,
    channel_adset,
    channel_ad,
    custom_channel_attribute_channel_group AS channel_group,
    custom_channel_attribute_meta_ads_campaign_id AS meta_ads_campaign_id,
    SUM(channel_spend)       AS channel_spend,
    SUM(channel_impressions) AS channel_impressions,
    SUM(channel_clicks)      AS channel_clicks
  FROM `%s.%s.%s`
  WHERE date >= '%s' AND date <= CURRENT_DATE()
  GROUP BY 1, 2, 3, 4, 5, 6, 7",
  project_id, dataset_name, tbl_name, start_date
)

message("\n=======================================================")
message("AD SPEND Full-Refresh (neue View, 1 Query, Smart Backoff)")
message("=======================================================")

page_sizes <- c(50000, 15000, 5000, 1000)
df <- NULL
for (attempt in seq_along(page_sizes)) {
  df <- tryCatch(
    {
      tb <- bigrquery::bq_project_query(project_id, query, quiet = TRUE)
      bigrquery::bq_table_download(tb, page_size = page_sizes[attempt], quiet = TRUE)
    },
    error = function(e) {
      warning("❌ Versuch ", attempt, " (page_size ", page_sizes[attempt], "): ", e$message)
      Sys.sleep(5 * attempt)
      NULL
    }
  )
  if (!is.null(df)) break
}
if (is.null(df)) stop("🚨 Spend-Download nach ", length(page_sizes), " Versuchen fehlgeschlagen!")

df <- df |>
  dplyr::mutate(
    date = as.Date(date),
    channel_spend = as.numeric(channel_spend),
    channel_clicks = as.numeric(channel_clicks),
    channel_impressions = as.numeric(channel_impressions)
  )
message(
  "✅ Download: ", format(nrow(df), big.mark = "."), " Zeilen (",
  format(min(df$date)), " bis ", format(max(df$date)), ")"
)

# 3. Lokal in Monats-Chunks splitten, atomar ersetzen
monat <- format(df$date, "%Y_%m")
for (m in sort(unique(monat))) {
  chunk_file <- file.path(data_dir, sprintf("adtribute_spend_%s.parquet", m))
  tmp <- paste0(chunk_file, ".tmp")
  arrow::write_parquet(df[monat == m, ], tmp)
  file.rename(tmp, chunk_file)
}
message("✅ ", length(unique(monat)), " Monats-Chunks geschrieben.")
message("\n✅ Spend-Pipeline-Durchlauf beendet!")
