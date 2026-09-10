# retouren_updater.R -- manueller Retouren-Abruf.
#
# Der Nachtlauf nutzt dieseoR::update_retouren_data() (Delta-faehig, mode
# "full"/"tail"); dieses Skript ist der Handgriff fuer einen Full-Sweep.
#
# Der API-Key kommt aus RETOUREN_API_KEY (~/.Renviron bzw. Server-Env) --
# vorher stand er hier im Klartext.
source("~/workspace/local.R")
library(dieseoR)

api_key <- Sys.getenv("RETOUREN_API_KEY")
if (!nzchar(api_key)) {
  stop("RETOUREN_API_KEY ist nicht gesetzt (.Renviron bzw. Server-Environment).")
}

raw_returns <- get_retouren_data(
  api_key  = api_key,
  base_url = "https://retoure-api.pammys.com/api/all-returns?per_page=100"
)
clean_returns <- clean_up_returns(raw_returns)
saveRDS(clean_returns, file = file.path(datadir, "returns/all_returns_cleaned.rds"))
