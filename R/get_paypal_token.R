#' @title Request PayPal OAuth 2.0 Access Token
#'
#' @description Authenticates with the PayPal REST API using Client Credentials
#' stored in the `.Renviron` file and retrieves a short-lived Bearer access token.
#'
#' @param client_id Character. The PayPal Client ID. Defaults to the environment variable `PAYPAL_CLIENT_ID`.
#' @param secret Character. The PayPal Secret. Defaults to the environment variable `PAYPAL_SECRET`.
#' @param base_url Character. The PayPal API base URL. Defaults to the environment variable `PAYPAL_BASE_URL`.
#'
#' @return Character string containing the access token.
#' @export
#'
#' @importFrom httr POST authenticate content stop_for_status
#'
#' @examples \dontrun{
#' token <- get_paypal_token()
#' }
get_paypal_token <- function(client_id = Sys.getenv("PAYPAL_CLIENT_ID"),
                             secret = Sys.getenv("PAYPAL_SECRET"),
                             base_url = Sys.getenv("PAYPAL_BASE_URL")) {
  if (client_id == "" || secret == "" || base_url == "") {
    stop("PayPal credentials not found. Please check your .Renviron file.")
  }

  # Endpunkt für die Token-Generierung
  token_url <- paste0(base_url, "/v1/oauth2/token")

  # Request senden
  response <- httr::POST(
    url = token_url,
    httr::authenticate(user = client_id, password = secret),
    body = list(grant_type = "client_credentials"),
    encode = "form"
  )

  # Robustes Error Handling
  if (httr::status_code(response) != 200) {
    stop(sprintf("Failed to get PayPal token. Status Code: %s", httr::status_code(response)))
  }

  # Token extrahieren
  token_data <- httr::content(response, as = "parsed", type = "application/json")

  return(token_data$access_token)
}
