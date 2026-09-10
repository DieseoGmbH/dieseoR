source("~/local.R")
library(devtools)
load_all()
library(dieseoR)

# 1. Credentials SICHER aus der .Renviron laden
client_id <- Sys.getenv("SHOPIFY_CLIENT_ID")
client_secret <- Sys.getenv("SHOPIFY_CLIENT_SECRET")

if (client_id == "" || client_secret == "") {
  stop("❌ Credentials nicht gefunden! Bitte prüfe deine ~/.Renviron Datei.")
}

# 2. Token generieren
message("Authentifiziere bei Shopify...")
my_shopify_token <- get_shopify_token(
  client_id = client_id,
  client_secret = client_secret
)

# 3. Liste der Endpunkte, die geupdatet werden sollen
endpoints_to_update <- c("orders", "checkouts", "products", "customers")

message(sprintf("\nStarte inkrementelles Update für %s Endpunkte...", length(endpoints_to_update)))

# 4. Inkrementelles Update durchführen
for (ep in endpoints_to_update) {
  message("\n---------------------------------------------------")
  message(">>> UPDATE ENDPUNKT: ", toupper(ep))
  message("---------------------------------------------------")

  tryCatch(
    {
      dieseoR::update_shopify_data(
        datadir = datadir,
        endpoint = ep,
        api_key = my_shopify_token
      )
    },
    error = function(e) {
      message("❌ Fehler beim Update von ", ep, ": ", e$message)
    }
  )

  Sys.sleep(1)
}

message("\n🎉 ALLE SHOPIFY UPDATES ABGESCHLOSSEN!")
