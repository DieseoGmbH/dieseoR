# ==============================================================================
# EINMAL-SKRIPT: Trustpilot-Datenluecke schliessen (17.08.2026)
# ==============================================================================
#
# HINTERGRUND
# Der Trustpilot-API-Zugang war vom 28.05. bis 17.08.2026 entrechtet (403 auf
# allen Endpunkten). In dieser Zeit hat die Nacht-Pipeline den Fehler nur per
# message() geloggt und die alte Datei weiter deployt -> 81 Tage stille Luecke.
#
# Nach der Key-Erneuerung lief `update_trustpilot_data()` einmal mit dem Default
# `pages_to_fetch = 5`. Das holte nur die neuesten 500 Reviews (11.-17.08.) und
# hat damit den Watermark auf den 17.08. gesetzt. Da die Funktion intern auf
# `created_at > max(created_at)` filtert, kann sie die verbleibende Luecke
# 28.05.-10.08. prinzipiell nicht mehr fuellen -- unabhaengig von pages_to_fetch.
# Genau dafuer ist dieses Skript da.
#
# STRATEGIE
# Nicht "N Seiten raten", sondern paginieren bis die Zieltiefe erreicht ist:
# Die API sortiert standardmaessig absteigend (verifiziert 17.08.: Seite 1 =
# 14.-17.08., Seite 30 = 07.07.), also blaettern wir vorwaerts, bis das aelteste
# Review einer Seite VOR dem Beginn der Luecke liegt. Danach Upsert per `id`.
#
# IDEMPOTENZ
# Mehrfach ausfuehrbar: Dedup laeuft ueber `id`, bestehende Zeilen gewinnen
# nicht "per Zufall", sondern der frischere API-Stand wird bevorzugt. Ein
# zweiter Lauf aendert nichts mehr.
#
# SICHERHEIT
# Vor dem Ueberschreiben wird ein Zeitstempel-Backup in ~/data/trustpilot/
# abgelegt. Die Datei wird erst geschrieben, wenn alle Pruefungen bestehen.
#
# AUFRUF
#   Rscript ~/git/dieseoR/scripts/produktiv/backfill_trustpilot_gap.R
# ==============================================================================

suppressMessages({
  library(dplyr)
  library(httr)
  library(lubridate)
})

TARGET_FILE <- path.expand("~/git/dashboard/data/cleaned_trustpilot.rds")
BACKUP_DIR <- path.expand("~/data/trustpilot")
MAX_PAGES <- 120L # harte Notbremse; die echte Abbruchbedingung ist datenbasiert
PER_PAGE <- 100L

bu <- Sys.getenv("TRUSTPILOT_BUSINESS_UNIT")
key <- Sys.getenv("TRUSTPILOT_API_KEY")
if (bu == "" || key == "") {
  stop("TRUSTPILOT_BUSINESS_UNIT oder TRUSTPILOT_API_KEY fehlt in der .Renviron.", call. = FALSE)
}

message("\n=== Trustpilot-Backfill [", Sys.time(), "] ===\n")

# ---------------------------------------------------------------------------
# 1. Ist-Zustand und Luecke bestimmen
# ---------------------------------------------------------------------------
if (!file.exists(TARGET_FILE)) {
  stop(sprintf("Zieldatei nicht gefunden: %s", TARGET_FILE), call. = FALSE)
}
df_existing <- readRDS(TARGET_FILE)
n_before <- nrow(df_existing)
cols_before <- names(df_existing)

vorhandene_tage <- unique(as_date(df_existing$created_at))
alle_tage <- seq(min(vorhandene_tage, na.rm = TRUE), Sys.Date(), by = "day")
fehlende_tage <- setdiff(alle_tage, vorhandene_tage) |> as_date()

if (length(fehlende_tage) == 0) {
  message("Keine fehlenden Tage gefunden -- nichts zu tun.")
  quit(save = "no", status = 0)
}

# Die zu schliessende Luecke ist der juengste zusammenhaengende Block, der noch
# nicht am heutigen Rand klebt. Wir gehen bewusst auf das Minimum aller
# fehlenden Tage im Jahr 2026, um auch aeltere Loecher mitzunehmen.
kandidaten <- fehlende_tage[fehlende_tage >= as_date("2026-01-01")]
if (length(kandidaten) == 0) {
  message("Keine fehlenden Tage in 2026 -- nichts zu tun.")
  quit(save = "no", status = 0)
}
luecke_von <- min(kandidaten)
message(sprintf(
  "Bestand:            %s Zeilen (%s bis %s)",
  format(n_before, big.mark = "."),
  format(min(df_existing$created_at, na.rm = TRUE), "%Y-%m-%d"),
  format(max(df_existing$created_at, na.rm = TRUE), "%Y-%m-%d")
))
message(sprintf("Fehlende Tage:      %d", length(fehlende_tage)))
message(sprintf("Blaettere zurueck bis mindestens: %s\n", luecke_von))

# ---------------------------------------------------------------------------
# 2. Paginieren bis zur Zieltiefe
# ---------------------------------------------------------------------------
url <- paste0("https://api.trustpilot.com/v1/business-units/", bu, "/reviews")
gesammelt <- list()
seite <- 1L
erreicht <- FALSE

while (seite <= MAX_PAGES && !erreicht) {
  res <- httr::RETRY(
    verb = "GET", url = url,
    query = list(page = seite, perPage = PER_PAGE, orderBy = "createdat.desc"),
    httr::add_headers(apikey = key),
    times = 4, pause_base = 2, pause_cap = 20, quiet = TRUE
  )

  if (httr::status_code(res) != 200) {
    stop(sprintf(
      "API-Fehler auf Seite %d - Status %s. Abbruch OHNE Schreiben.",
      seite, httr::status_code(res)
    ), call. = FALSE)
  }

  parsed <- httr::content(res, as = "parsed", type = "application/json")
  if (length(parsed$reviews) == 0) {
    message(sprintf("  Seite %3d: leer -- Ende der Daten erreicht.", seite))
    break
  }

  gesammelt[[seite]] <- parsed$reviews |>
    purrr::map(~ as.data.frame(t(unlist(.x)), stringsAsFactors = FALSE)) |>
    bind_rows() |>
    as_tibble()

  seiten_daten <- as_date(gesammelt[[seite]]$createdAt)
  aeltestes <- min(seiten_daten, na.rm = TRUE)

  if (seite %% 10 == 0 || seite <= 3) {
    message(sprintf(
      "  Seite %3d: %3d Reviews, zurueck bis %s",
      seite, nrow(gesammelt[[seite]]), aeltestes
    ))
  }

  # Abbruchbedingung: wir sind hinter den Beginn der Luecke geblaettert
  if (aeltestes < luecke_von) {
    erreicht <- TRUE
    message(sprintf("  Seite %3d: Zieltiefe erreicht (%s < %s).", seite, aeltestes, luecke_von))
  }
  seite <- seite + 1L
}

if (!erreicht) {
  warning(sprintf(paste0(
    "Zieltiefe %s wurde in %d Seiten NICHT erreicht. Es werden nur die ",
    "geholten Daten eingespielt -- die Luecke bleibt womoeglich teilweise offen. ",
    "MAX_PAGES erhoehen und erneut laufen lassen (idempotent)."
  ), luecke_von, MAX_PAGES))
}

df_raw <- bind_rows(gesammelt)
message(sprintf(
  "\nVon der API geholt: %s Reviews aus %d Seiten\n",
  format(nrow(df_raw), big.mark = "."), length(gesammelt)
))

# ---------------------------------------------------------------------------
# 3. Bereinigen -- exakt dieselbe Funktion wie in der Nacht-Pipeline
# ---------------------------------------------------------------------------
df_clean <- dieseoR::clean_up_trustpilot(df_raw)

# Schema-Guard: bind_rows() wuerde bei abweichenden Spalten stillschweigend
# NA-Spalten anlegen. Das faellt sonst erst im Dashboard auf.
neu_extra <- setdiff(names(df_clean), cols_before)
neu_fehlt <- setdiff(cols_before, names(df_clean))
if (length(neu_extra) > 0 || length(neu_fehlt) > 0) {
  message("⚠️  Schema-Abweichung zwischen API-Daten und Bestand:")
  if (length(neu_extra) > 0) message("   Nur in API-Daten: ", paste(neu_extra, collapse = ", "))
  if (length(neu_fehlt) > 0) message("   Nur im Bestand:   ", paste(neu_fehlt, collapse = ", "))
  message("   -> Es werden ausschliesslich die Bestandsspalten uebernommen.")
  df_clean <- df_clean |> select(any_of(cols_before))
}

# ---------------------------------------------------------------------------
# 4. Upsert per `id`
# ---------------------------------------------------------------------------
# Achtung: die Dedup-Logik in update_trustpilot_data() referenziert `review_id`
# -- diese Spalte existiert im bereinigten Schema NICHT (verifiziert 17.08.2026),
# weshalb dort still der Fallback distinct() ueber alle Spalten greift. Hier
# deduplizieren wir bewusst ueber `id` und geben dem frischeren API-Stand den
# Vorrang, damit nachtraeglich geaenderte Reviews (Sterne, Likes) aktualisiert
# werden statt doppelt zu landen.
stopifnot("id" %in% names(df_existing), "id" %in% names(df_clean))

df_combined <- bind_rows(df_clean, df_existing) |>
  distinct(id, .keep_all = TRUE) |>
  arrange(created_at)

n_after <- nrow(df_combined)
n_neu <- n_after - n_before

# ---------------------------------------------------------------------------
# 5. Pruefungen VOR dem Schreiben
# ---------------------------------------------------------------------------
message("=== Pruefungen ===")
ok <- TRUE

pruefe <- function(label, bedingung, detail = "") {
  message(sprintf("  [%s] %-42s %s", if (bedingung) "OK" else "FEHLER", label, detail))
  if (!bedingung) ok <<- FALSE
}

pruefe(
  "Keine Zeilen verloren", n_after >= n_before,
  sprintf("%s -> %s", format(n_before, big.mark = "."), format(n_after, big.mark = "."))
)
pruefe("Keine doppelten ids", n_distinct(df_combined$id) == n_after)
pruefe(
  "Spaltenzahl unveraendert", identical(sort(names(df_combined)), sort(cols_before)),
  sprintf("%d Spalten", ncol(df_combined))
)
pruefe("created_at ohne NA", !any(is.na(df_combined$created_at)))

# Bewusst KEIN harter Gate: ein einzelner legitim reviewfreier Tag wuerde sonst
# den ganzen Schreibvorgang blockieren, obwohl der Backfill korrekt gelaufen ist.
# Wir schreiben den Fortschritt und melden, was offen bleibt -- das Skript ist
# idempotent und kann danach erneut laufen.
rest <- setdiff(
  seq(luecke_von, Sys.Date(), by = "day") |> as_date(),
  unique(as_date(df_combined$created_at))
) |> as_date()
if (length(rest) == 0) {
  message("  [OK]     Luecke vollstaendig geschlossen")
} else {
  message(sprintf(
    "  [HINWEIS] %d Tage ohne Reviews: %s%s",
    length(rest), paste(utils::head(rest, 5), collapse = ", "),
    if (length(rest) > 5) " ..." else ""
  ))
  message("            (kann echte reviewfreie Tage enthalten -- pruefen, nicht blind nachladen)")
}

if (!ok) {
  stop("Mindestens eine Pruefung ist fehlgeschlagen -- es wurde NICHTS geschrieben.", call. = FALSE)
}

# ---------------------------------------------------------------------------
# 6. Backup, dann schreiben
# ---------------------------------------------------------------------------
if (!dir.exists(BACKUP_DIR)) dir.create(BACKUP_DIR, recursive = TRUE)
backup <- file.path(BACKUP_DIR, sprintf(
  "cleaned_trustpilot_backup_%s.rds",
  format(Sys.time(), "%Y%m%d_%H%M%S")
))
file.copy(TARGET_FILE, backup, overwrite = FALSE)
message(sprintf("\nBackup angelegt: %s", backup))

saveRDS(df_combined, TARGET_FILE)
message(sprintf(
  "✅ Geschrieben: %s (%s Zeilen, davon %s neu)",
  TARGET_FILE, format(n_after, big.mark = "."), format(n_neu, big.mark = ".")
))
message("\nNaechster Schritt: trustpilot_tokens.rds neu bauen (NLP), sonst bleiben die")
message("Sentiment-Karten im Dashboard fuer den neuen Zeitraum leer.")
