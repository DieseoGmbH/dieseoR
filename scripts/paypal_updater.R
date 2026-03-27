# Initialer historischer PayPal-Load (Nur einmal ausfuehren!)

# Lade unser eigenes Package und die Workspace-Variablen
devtools::load_all("~/git/dieseoR")
source("~/workspace/local.R") # Laedt die 'datadir' Variable

update_paypal_data(datadir = datadir)

paypal_data <- readRDS(file.path(datadir, "paypal/all_paypal_transactions.rds"))

paypal_data |>
  clean_up_paypal() |>
  saveRDS(file.path(datadir, "paypal/all_paypal_transactions_cleaned.rds"))
