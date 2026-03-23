#' @title Generate Shopify Access Token (OAuth Client Credentials)
#'
#' @description Authentifiziert sich bei der Shopify API ueber den Client Credentials Grant
#' und tauscht Client ID sowie Client Secret (`shpss_`) gegen ein temporaeres Access Token ein.
#'
#' @param shop_url Character. Die Shopify-Shop-URL (Standard: "pummmys.myshopify.com").
#' @param client_id Character. Die Client ID der App aus dem Developer Dashboard.
#' @param client_secret Character. Das Client Secret der App (beginnt mit `shpss_`).
#'
#' @return Character. Das generierte Access Token fuer nachfolgende API-Requests.
#'
#' @importFrom httr POST content stop_for_status
#' @importFrom jsonlite fromJSON
#' @importFrom stringr str_c
#' @export
#'
#' @examples \dontrun{
#' token <- get_shopify_token(
#'   client_id = Sys.getenv("SHOPIFY_CLIENT_ID"),
#'   client_secret = Sys.getenv("SHOPIFY_CLIENT_SECRET")
#' )
#' }
get_shopify_token <- function(shop_url = "pummmys.myshopify.com",
                              client_id,
                              client_secret) {
  if (missing(client_id) || missing(client_secret)) {
    stop("Fehler: client_id und client_secret muessen uebergeben werden.")
  }

  req_url <- stringr::str_c("https://", shop_url, "/admin/oauth/access_token")

  message("Generiere neues Shopify Access Token...")

  # POST-Request an Shopify, um das Token zu generieren
  response <- httr::POST(
    url = req_url,
    body = list(
      client_id = client_id,
      client_secret = client_secret,
      grant_type = "client_credentials"
    ),
    encode = "json"
  )

  # Fehlerhandling
  httr::stop_for_status(response, task = "Generierung des Shopify Access Tokens")

  # Antwort parsen
  raw_content <- httr::content(response, as = "text", encoding = "UTF-8")
  parsed_data <- jsonlite::fromJSON(raw_content)

  message("Token erfolgreich generiert.")

  return(parsed_data$access_token)
}
