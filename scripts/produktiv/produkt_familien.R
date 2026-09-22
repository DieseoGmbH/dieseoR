# ============================================================================
# produkt_familien.R -- Produkttitel -> Produktfamilie
# ============================================================================
# Warum es das gibt: derselbe Schuh liegt in Shopify unter bis zu sieben
# Titeln ("Originals Woman", "Originals Women", "Pammys™ - Originals",
# "Pummys™ - Original", "Pummys™ - Das Original", ...). Eine Auswertung je
# `product_title` zerlegt ein Produkt deshalb in mehrere Zeilen und macht die
# Produktperformance unlesbar -- genau das Problem im alten Tab.
#
# Aufloesung in zwei Stufen:
#   1. Override-CSV (`produkt_familien_override.csv`) -- hat immer Vorrang,
#      von Hand pflegbar fuer alles, was die Regeln falsch oder gar nicht
#      treffen.
#   2. Regelwerk unten -- erste passende Regel gewinnt. Faengt automatisch
#      neue Titel ab (Jahressale-Varianten, Jahreszahlen, Marken-Praefix).
#
# Titel, die keine Regel trifft, landen in der Familie "unbekannt" und werden
# vom Builder als Warnung mit Umsatz ausgewiesen -- dann hier oder in der
# Override-CSV nachziehen.
#
# WICHTIG: Reihenfolge der Regeln ist Semantik. Spezifisch vor allgemein
# ("step-ins waterproof" vor "step-ins", "originals" vor "pro").

# --- Normalisierung: alles weg, was nur Schreibweise ist ---------------------
pf_normalisieren <- function(x) {
  x |>
    tolower() |>
    gsub("[™®]", " ", x = _) |>
    gsub("\\(copy\\)", " ", x = _) |>
    # Verkaufs-/Preis-Etiketten, keine Produkteigenschaft
    gsub("jahressale|gratis|geschenkt|\\bneon\\b|\\-r\\b", " ", x = _) |>
    # Marken-Praefix in allen Schreibweisen
    gsub("\\b(pammys|pummys)\\b", " ", x = _) |>
    gsub("\\bpillowsteps\\b", " ", x = _) |>
    # Jahreszahlen und Versionsnummern
    gsub("\\b(19|20)[0-9]{2}\\b", " ", x = _) |>
    gsub("[0-9]+\\.[0-9]+", " ", x = _) |>
    gsub("[^a-zäöüß ]", " ", x = _) |>
    gsub("\\s+", " ", x = _) |>
    trimws()
}

# --- Regelwerk: erste Treffer gewinnt ---------------------------------------
# muster = Regex auf den normalisierten Titel, key = Familien-Schluessel,
# label = Anzeigename im Dashboard.
pf_regeln <- tibble::tribble(
  ~muster, ~key, ~label,
  # Nicht-Produkte zuerst -- sonst verschmutzen sie echte Familien
  "gutschein", "gutschein", "Gutschein",
  "strumpfhose|leggings|wintersocken", "textil", "Strumpfwaren",
  # Spezifische Varianten vor der Basisfamilie
  "step.?ins?.*waterproof|waterproof.*step", "step_ins_wp", "Step-Ins Waterproof",
  "step.?ins?", "step_ins", "Step-Ins",
  "snowboot", "snowboots", "Snowboots",
  "mallow jelly", "mallow_jelly", "Mallow Jelly Slide",
  "mallow slide", "mallow_slide", "Mallow Slide",
  "mallow sandal", "mallow_sandal", "Mallow Sandal",
  "retro sneaker", "retro_sneaker", "Retro Sneaker",
  "ballet sneaker", "ballet_sneaker", "Ballet Sneaker",
  "court", "court_sneaker", "Court Sneaker",
  "frame sneaker", "frame_sneaker", "Frame Sneaker",
  "sky runner", "sky_runner", "Sky Runner",
  "chunky step", "chunky_step", "Chunky Step",
  "charge ?flow", "charge_flow", "Charge Flow",
  "sip ?flow", "sip_flow", "Sip Flow",
  "jelly flow", "jelly_flow", "Jelly Flow",
  "daily loop", "daily_loop", "Daily Loop",
  "lift queen", "lift_queen", "Lift Queen",
  "featherfeel", "featherfeel", "Featherfeel",
  "flip ?flop", "flipflop", "FlipFlop",
  "keychain", "keychain", "Keychain",
  "puffy bag", "puffy_bag", "Puffy Bag",
  "print|poster", "print", "Print / Poster",
  # "originals" MUSS vor "pro" stehen: "Originals Pro" ist eine Originals-Variante
  "original", "originals", "Originals",
  "belt", "belt", "Belt",
  "hearth", "hearth", "Hearth",
  "huggy|\\bhug\\b", "huggy", "Huggy",
  "ember", "ember", "Ember",
  "cozy", "cozy", "Cozy",
  "chelsea", "chelsea", "Chelsea",
  "barfußschuhe|\\bwalk\\b|\\brun\\b", "barfuss", "Barfußschuhe",
  "klett", "klett", "Klett",
  "limited", "limited", "Limited Edition",
  "sleek", "sleek", "Sleek",
  "trail", "trail", "Trail",
  "\\bpro\\b", "pro", "Pro",
  # Kleine Alt-Linien der Pummys-Aera, bewusst als eine Familie gebuendelt
  "soft|filz|chic|comfy|teddy|premium|native|cushy|home|utune",
  "legacy", "Alt-Linien (Pummys)"
)

# --- Zielgruppe aus dem Titel ------------------------------------------------
# Reihenfolge zaehlt: "women" enthaelt "men".
pf_zielgruppe <- function(titel) {
  t <- tolower(titel)
  dplyr::case_when(
    grepl("kids|children|kinder", t) ~ "Kinder",
    grepl("women|woman|damen", t) ~ "Damen",
    grepl("\\bmen\\b|herren", t) ~ "Herren",
    TRUE ~ "Unisex"
  )
}

#' Produkttitel auf Familien abbilden
#'
#' @param titel Character-Vektor der Shopify-Produkttitel.
#' @param override_pfad Pfad zur Override-CSV (Spalten: product_title,
#'   produkt_key, label). Fehlt die Datei, greifen nur die Regeln.
#' @return data.frame mit product_title, produkt_key, label, zielgruppe, quelle.
pf_map <- function(titel, override_pfad = NULL) {
  titel <- unique(titel[!is.na(titel) & titel != ""])
  norm <- pf_normalisieren(titel)

  key <- rep(NA_character_, length(titel))
  lab <- rep(NA_character_, length(titel))
  for (i in seq_len(nrow(pf_regeln))) {
    treffer <- is.na(key) & grepl(pf_regeln$muster[i], norm)
    key[treffer] <- pf_regeln$key[i]
    lab[treffer] <- pf_regeln$label[i]
  }
  quelle <- ifelse(is.na(key), "unbekannt", "regel")
  key[is.na(key)] <- "unbekannt"
  lab[is.na(lab)] <- "Nicht zugeordnet"

  out <- data.frame(
    product_title = titel, produkt_key = key, label = lab,
    zielgruppe = pf_zielgruppe(titel), quelle = quelle,
    stringsAsFactors = FALSE
  )

  # Override hat Vorrang -- immer, auch gegen einen Regeltreffer.
  if (!is.null(override_pfad) && file.exists(override_pfad)) {
    ov <- utils::read.csv(override_pfad, stringsAsFactors = FALSE)
    i <- match(out$product_title, ov$product_title)
    tr <- !is.na(i)
    out$produkt_key[tr] <- ov$produkt_key[i[tr]]
    out$label[tr] <- ov$label[i[tr]]
    out$quelle[tr] <- "override"
  }
  out
}
