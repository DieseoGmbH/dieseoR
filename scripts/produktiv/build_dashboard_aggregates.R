# ============================================================================
# build_dashboard_aggregates.R (MINIMAL VERSION)
# ============================================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(DBI)
  library(duckdb)
})

DASH_DATA <- "~/git/dashboard/data"
message("[", Sys.time(), "] Verbinde mit DuckDB für Produkt-Performance...")

# 1. Lokale Daten laden
returns <- readRDS(file.path(DASH_DATA, "all_returns_cleaned.rds")) |>
  mutate(shopify_order_id = as.numeric(shopify_order_id))

# 2. Verbindung zur DuckDB -- READ_ONLY: der Serving-Mart wird NICHT geschrieben
#    (verhindert Locks/haengende Verbindungen, die Folge-Schritte/Re-Runs kaputt
#    machen). Kein on.exit() (bekannter Bug beim Sourcen) -> tryCatch/finally.
con <- DBI::dbConnect(duckdb::duckdb(),
  dbdir = file.path(DASH_DATA, "shopify.duckdb"),
  read_only = TRUE
)

tryCatch({
  # 3. Retouren als VIRTUELLE Tabelle registrieren (kein Schreibzugriff noetig)
  returns_for_join <- returns |>
    filter(type == "return") |>
    select(shopify_order_id, type) |>
    distinct()
  duckdb::duckdb_register(con, "returns_temp", returns_for_join)

  shopify_db <- dplyr::tbl(con, "orders")
  returns_db <- dplyr::tbl(con, "returns_temp")

  # 4. Aggregation in der Datenbank
  shopifys_without_returns <- shopify_db |>
    select(
      order_id, financial_status, item_gross_revenue, quantity,
      product_title, product_title_with_variant, variant_title
    ) |>
    rename(shopify_order_id = order_id) |>
    left_join(returns_db, by = "shopify_order_id") |>
    filter(is.na(type)) |>
    filter(financial_status %in% c("paid", "partially_paid")) |>
    select(-type) |>
    collect() |> # 🚀 Ab hier sind wir sicher im RAM!
    mutate(
      product_title_with_variant = str_replace_all(product_title_with_variant, "Pammys™ - ", ""),
      product_title_with_variant = str_replace_all(product_title_with_variant, "PillowSteps ", ""),
      product_title_with_variant = str_replace_all(product_title_with_variant, "Jahressale ", ""),
      product_title_with_variant = str_remove(product_title_with_variant, "/.*$") |> str_trim()
    )

  product_choices_ranked <- shopifys_without_returns |>
    filter(!is.na(product_title) & product_title != "") |>
    group_by(product_title) |>
    summarise(total_sold = sum(quantity, na.rm = TRUE), .groups = "drop") |>
    arrange(desc(total_sold)) |>
    pull(product_title)

  # 5. Speichern
  saveRDS(shopifys_without_returns, file.path(DASH_DATA, "shopifys_without_returns.rds"))
  saveRDS(product_choices_ranked, file.path(DASH_DATA, "product_choices_ranked.rds"))
}, finally = {
  # Verbindung IMMER schliessen -> keine haengenden Locks fuer Folge-Schritte
  try(duckdb::duckdb_unregister(con, "returns_temp"), silent = TRUE)
  try(DBI::dbDisconnect(con, shutdown = TRUE), silent = TRUE)
})

# ============================================================================
# 6. Bearbeiter-Zuordnung Rueckgaben <-> Zendesk  (NEU 14.08.2026)
# ============================================================================
# Das Retourenportal protokolliert NICHT, wer einen Vorgang bearbeitet hat.
# Ueber die Zendesk-Tickets laesst sich aber zumindest ermitteln, wer den
# SUPPORT-KONTAKT zur selben Bestellung hatte. Join-Schluessel ist die
# 9-stellige Shopify-Bestellnummer (returns$order_number ohne "#" <->
# tickets$bestellnummer).
#
# ACHTUNG, Vorgeschichte: bis zum 14.08.2026 hat clean_up_zendesk() die
# Bestellnummer per Regex auf 8 Stellen abgeschnitten (Shopify ist laengst
# 9-stellig). Dadurch matchten von 438.661 Retouren nur 354 -- der Join sah
# unmoeglich aus, war aber bloss kaputt. Wenn diese Datei ploetzlich leer
# ist, ist als Erstes zu pruefen, ob eine alte dieseoR-Version installiert
# ist: `table(nchar(tickets$bestellnummer))` muss 9 liefern, nicht 8.
message("[", Sys.time(), "] Baue Bearbeiter-Zuordnung (Retouren x Zendesk)...")

returns_agent_map <- tryCatch(
  {
    tickets <- readRDS(file.path(DASH_DATA, "all_tickets_selected.rds"))

    if (!"bestellnummer" %in% names(tickets)) {
      stop("Spalte 'bestellnummer' fehlt in all_tickets_selected.rds")
    }
    laengen <- table(nchar(tickets$bestellnummer))
    if (!"9" %in% names(laengen)) {
      warning(
        "Keine 9-stelligen Bestellnummern in den Tickets -- ",
        "vermutlich laeuft noch die alte clean_up_zendesk()-Version. ",
        "Die Bearbeiter-Zuordnung bleibt dann leer."
      )
    }

    # Nur echte Personen. "AI" ist der Bot (bearbeitet mit 137k Tickets rund
    # die Haelfte allein und ist der Grund fuer die begrenzte Abdeckung),
    # "pammys_support" ein Sammelaccount, rein numerische Werte sind Agenten
    # ohne Eintrag in der id_name_map.
    keine_person <- c("AI", "pammys_support")
    tickets |>
      filter(
        !is.na(bestellnummer), !is.na(assignee_id),
        !assignee_id %in% keine_person,
        !str_detect(assignee_id, "^[0-9]+$"),
        # Plausibilitaet: echte Shopify-Bestellnummern beginnen mit "12"
        # (121xxxxxx-122xxxxxx). Der Fallback \\b\\d{8}\\b im Extraktions-
        # Regex faengt sonst auch Hausnummern, Datumsketten o. ae. ein
        # ("00000000", "04250236") und wuerde Falsch-Joins erzeugen.
        str_detect(bestellnummer, "^12[0-9]{6,7}$")
      ) |>
      # Regel bei mehreren Tickets zur selben Bestellung: der ZULETZT
      # eroeffnete Kontakt gewinnt -- das ist der Stand, der zum Vorgang passt.
      arrange(desc(created_at)) |>
      group_by(bestellnummer) |>
      summarise(
        bearbeiter = first(assignee_id),
        tickets_n = n(),
        bearbeiter_n = n_distinct(assignee_id), # >1 = Vorgang wanderte
        .groups = "drop"
      )
  },
  error = function(e) {
    message("FEHLER bei der Bearbeiter-Zuordnung: ", conditionMessage(e))
    tibble::tibble(
      bestellnummer = character(), bearbeiter = character(),
      tickets_n = integer(), bearbeiter_n = integer()
    )
  }
)

saveRDS(returns_agent_map, file.path(DASH_DATA, "returns_agent_map.rds"))

abdeckung <- {
  onc <- str_remove(returns$order_number, "^#")
  round(100 * mean(onc %in% returns_agent_map$bestellnummer), 1)
}
message(
  "[", Sys.time(), "] returns_agent_map.rds: ",
  nrow(returns_agent_map), " Bestellungen, ",
  abdeckung, " % der Vorgaenge zuordenbar."
)

message("[", Sys.time(), "] Fertig. shopifys_without_returns.rds, product_choices_ranked.rds und returns_agent_map.rds erstellt.")
