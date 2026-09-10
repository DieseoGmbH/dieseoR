# ============================================================================
# build_schema_snapshot.R
# ============================================================================
# Schreibt einen aktuellen Schema-Snapshot aller relevanten Datenquellen nach
# ~/git/dieseoR/scripts/ki_readmes/schema_snapshot.md — als Kontext-Dokument für
# KI-Chats, damit Spaltennamen/-typen nie halluziniert werden müssen.
#
# Bewusst KEINE Beispielwerte im Output (PII-Schutz: Kundendaten aus Zendesk,
# PayPal, Shopify landen sonst in KI-Prompts). Nur: Spaltenname, Typ, Zeilzahl.
#
# Aufruf: Rscript build_schema_snapshot.R  (oder via source() in der Pipeline)
# ============================================================================

suppressPackageStartupMessages({
  library(DBI)
  library(duckdb)
})

OUT_DIR <- path.expand("~/git/dieseoR/scripts/ki_readmes")
if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)
OUT_FILE <- file.path(OUT_DIR, "schema_snapshot.md")
DASH_DATA <- path.expand("~/git/dashboard/data")
DATADIR <- path.expand("~/data")

lines <- c(
  "# SCHEMA SNAPSHOT: Pammys Data Pipeline",
  "",
  sprintf(
    "Automatisch generiert von `build_schema_snapshot.R` am **%s**.",
    format(Sys.time(), "%d.%m.%Y %H:%M")
  ),
  "Dieses Dokument ist die verbindliche Referenz für Spaltennamen und Typen.",
  "Erfinde keine Spalten, die hier nicht stehen. Keine Beispielwerte enthalten (PII-Schutz).",
  ""
)

add <- function(...) lines <<- c(lines, ...)

# Rendert einen Data Frame (Spalten: name, type) als Markdown-Tabelle
md_schema_table <- function(df) {
  c("| Spalte | Typ |", "|---|---|", sprintf("| `%s` | %s |", df$name, df$type))
}

# --- Helper: DuckDB ---------------------------------------------------------
describe_duckdb <- function(db_path, label) {
  add(sprintf("## DuckDB: %s", label), "", sprintf("Pfad: `%s`", db_path), "")
  tryCatch(
    {
      con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
      on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

      tables <- DBI::dbListTables(con)
      if (length(tables) == 0) add("*(keine Tabellen)*", "")

      for (tbl in tables) {
        n_rows <- DBI::dbGetQuery(con, sprintf('SELECT COUNT(*) AS n FROM "%s"', tbl))$n
        info <- DBI::dbGetQuery(con, sprintf("PRAGMA table_info('%s')", tbl))
        add(
          sprintf("### Tabelle `%s` (%s Zeilen)", tbl, format(n_rows, big.mark = ".", decimal.mark = ",", scientific = FALSE)), "",
          md_schema_table(data.frame(name = info$name, type = info$type)), ""
        )
      }
    },
    error = function(e) {
      add(sprintf("⚠️ Konnte DuckDB nicht lesen: %s", conditionMessage(e)), "")
    }
  )
}

# --- Helper: RDS ------------------------------------------------------------
describe_rds <- function(rds_path, label) {
  add(sprintf("### RDS: `%s`", label), "", sprintf("Pfad: `%s`", rds_path), "")
  tryCatch(
    {
      if (!file.exists(rds_path)) stop("Datei existiert nicht.")
      obj <- readRDS(rds_path)

      if (is.data.frame(obj)) {
        types <- vapply(obj, function(col) paste(class(col), collapse = "/"), character(1))
        add(
          sprintf(
            "Data Frame mit **%s Zeilen** × %d Spalten.",
            format(nrow(obj), big.mark = ".", decimal.mark = ",", scientific = FALSE), ncol(obj)
          ), "",
          md_schema_table(data.frame(name = names(obj), type = unname(types))), ""
        )
      } else {
        add(sprintf(
          "Kein Data Frame: `%s` der Länge %s.",
          paste(class(obj), collapse = "/"), format(length(obj), big.mark = ".", decimal.mark = ",", scientific = FALSE)
        ), "")
      }
      rm(obj)
      invisible(gc(verbose = FALSE))
    },
    error = function(e) {
      add(sprintf("⚠️ Konnte RDS nicht lesen: %s", conditionMessage(e)), "")
    }
  )
}

# --- Helper: Parquet-Ordner (Arrow, liest nur Metadaten — kein RAM-Risiko) ---
describe_parquet_dir <- function(dir_path, label) {
  add(sprintf("## Parquet-Dataset: %s", label), "", sprintf("Pfad: `%s`", dir_path), "")
  tryCatch(
    {
      if (!dir.exists(dir_path)) stop("Ordner existiert nicht.")
      ds <- arrow::open_dataset(dir_path)
      schema <- ds$schema
      types <- vapply(schema$fields, function(f) f$type$ToString(), character(1))
      add(
        sprintf("%d Parquet-Files, %d Spalten.", length(ds$files), schema$num_fields), "",
        md_schema_table(data.frame(name = names(ds), type = types)), ""
      )
    },
    error = function(e) {
      add(sprintf("⚠️ Konnte Parquet-Dataset nicht lesen: %s", conditionMessage(e)), "")
    }
  )
}

# ============================================================================
# 1. DuckDB-Datenbanken
# ============================================================================
message("[", Sys.time(), "] Snapshot: DuckDB-Schemata...")
describe_duckdb(
  file.path(DATADIR, "shopify", "shopify.duckdb"),
  "Shopify Master (Source of Truth)"
)
describe_duckdb(
  file.path(DASH_DATA, "shopify.duckdb"),
  "Dashboard Data Mart (schlanker Nacht-Schnitt)"
)

# ============================================================================
# 2. Serving-RDS (Dashboard) + Masterdateien
# ============================================================================
message("[", Sys.time(), "] Snapshot: RDS-Dateien...")
add("## RDS-Dateien (Dashboard-Serving)", "")

rds_serving <- c(
  "all_tickets_selected.rds",
  "all_returns_cleaned.rds",
  "cleaned_trustpilot.rds",
  "all_paypal_transactions_cleaned.rds",
  "trustpilot_themes.rds",
  "trustpilot_tokens.rds",
  "shopifys_without_returns.rds",
  "product_choices_ranked.rds",
  "plz_coords_df.rds"
)
for (f in rds_serving) describe_rds(file.path(DASH_DATA, f), f)

add("## RDS-Dateien (Master in ~/data/)", "")
rds_master <- c(
  "meta/meta_daily_request.rds",
  "meta/meta_hourly_request.rds",
  "paypal/all_paypal_transactions_cleaned.rds"
)
for (f in rds_master) describe_rds(file.path(DATADIR, f), f)

# ============================================================================
# 3. Adtribute Parquet-Master
# ============================================================================
message("[", Sys.time(), "] Snapshot: Parquet-Datasets...")
describe_parquet_dir(
  file.path(DATADIR, "adtribute_parquet_chunks_full"),
  "Adtribute Touchpoints COMPREHENSIVE (Master, 92 Spalten)"
)
describe_parquet_dir(
  file.path(DATADIR, "adtribute_spend_chunks"),
  "Adtribute Spend"
)

# ============================================================================
# 4. Schreiben
# ============================================================================
writeLines(lines, OUT_FILE)
message("[", Sys.time(), "] ✅ Schema-Snapshot geschrieben nach: ", OUT_FILE)
