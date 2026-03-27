# Initialer historischer PayPal-Load (Nur einmal ausfuehren!)

# Lade unser eigenes Package und die Workspace-Variablen
devtools::load_all("~/git/dieseoR")
source("~/workspace/local.R") # Laedt die 'datadir' Variable

update_trustpilot_data(datadir = datadir)
