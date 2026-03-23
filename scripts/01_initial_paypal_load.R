# Initialer historischer PayPal-Load (Nur einmal ausfuehren!)

# Lade unser eigenes Package und die Workspace-Variablen
devtools::load_all("~/git/dieseoR")
source("~/workspace/local.R") # Laedt die 'datadir' Variable

# 1. Ziel-Ordner definieren und ggf. erstellen
paypal_dir <- file.path(datadir, "paypal")
if (!dir.exists(paypal_dir)) {
  dir.create(paypal_dir, recursive = TRUE)
  message("Verzeichnis ~/data/paypal erstellt.")
}

# 2. Token holen
token <- get_paypal_token()

# 3. Zeitraum definieren (z. B. das gesamte letzte Jahr)
# ACHTUNG: Das dauert ein paar Minuten, da die Funktion nun Tag für Tag chunked!
start_date <- Sys.Date() - 365
end_date <- Sys.Date()

# 4. Daten ziehen
df_historical <- get_paypal_transactions(
  token = token,
  start_date = start_date,
  end_date = end_date
)

# 5. Daten als Master-RDS abspeichern
file_path <- file.path(paypal_dir, "all_paypal_transactions.rds")
saveRDS(df_historical, file_path)

message(sprintf(
  "Initial Load erfolgreich! %s Zeilen gespeichert unter: %s",
  nrow(df_historical), file_path
))
