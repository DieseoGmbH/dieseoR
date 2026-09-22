# Produktfamilien — Pflege

`produkt_familien.R` bildet Shopify-Produkttitel auf Produktfamilien ab.
Verbraucher: `build_product_performance.R` (Nacht-Pipeline, Schritt 9d) und
darüber der Dashboard-Tab „Produktperformance".

## Warum es das gibt

Derselbe Schuh liegt in Shopify unter bis zu 36 Titeln — allein „Originals"
erscheint als `Originals Woman`, `Originals Women`, `Pammys™ - Originals`,
`Pummys™ - Original`, `Pummys™ - Das Original`, `Pammys Originals NEON` …
Eine Auswertung je `product_title` zerlegt ein Produkt in viele Zeilen; ein
Top-15-Chart zeigt dann fünfmal dasselbe Produkt.

## Zwei Stufen

1. **Override-CSV** (`produkt_familien_override.csv`) — hat immer Vorrang.
   Spalten: `product_title,produkt_key,label`. Hier eintragen, was die Regeln
   falsch oder gar nicht treffen. Die Datei startet leer; aktuell braucht sie
   keinen Eintrag.
2. **Regelwerk** in `produkt_familien.R` — erste passende Regel gewinnt.
   Fängt neue Titel automatisch ab (Jahressale-Varianten, Jahreszahlen,
   Marken-Präfix `Pammys`/`Pummys`/`PillowSteps`).

**Reihenfolge ist Semantik.** Spezifisch vor allgemein: `step-ins waterproof`
vor `step-ins`, `originals` vor `pro` (sonst würde „Originals Pro" zu `pro`).

## Neues Produkt aufgetaucht?

`build_product_performance.R` warnt im Nacht-Log, sobald ein Titel keiner
Regel entspricht, und nennt ihn beim Namen:

```
Produktfamilien: 2 Titel ohne Zuordnung -> ... : Sip Flow | City Court
```

Dann entweder eine Regel in `produkt_familien.R` ergänzen (bevorzugt, fängt
künftige Schreibweisen gleich mit) oder eine Zeile in die Override-CSV.
Ohne Zuordnung landet der Titel in der Familie `unbekannt` — er verschwindet
nicht, fällt im Dashboard aber als „Nicht zugeordnet" auf.

Stand 18.09.2026: 181 Titel, 38 Familien, 100 % des Umsatzes zugeordnet.
