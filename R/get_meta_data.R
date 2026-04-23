#' @title Lade Meta Ads Performance auf Ad-Ebene (Daily oder Hourly)
#' @description Ruft Insights (Spend, Clicks, Impressions) auf Ad-Ebene über die
#' Meta Marketing API ab. Unterstützt tägliche und stündliche Granularität.
#' Chunking passt sich automatisch an (30-Tage-Chunks für `daily`, 1-Tag-Chunks
#' für `hourly`, da Meta `time_increment=1` nicht mit Stunden-Breakdowns erlaubt).
#' Das Output-Schema entspricht dem jeweiligen Master-Layout:
#'   * `daily`  → `date_start` (Date) + `date_stop` (chr, wie Meta liefert)
#'   * `hourly` → `date` (Date, aus `date_start` umbenannt) + `hour` (chr)
#' @param start_date Startdatum ("YYYY-MM-DD" oder Date).
#' @param end_date Enddatum ("YYYY-MM-DD" oder Date).
#' @param granularity Entweder `"daily"` (Default) oder `"hourly"`.
#' @return `tibble` im jeweiligen Master-Schema.
#' @family adribute
#' @importFrom httr2 request req_auth_bearer_token req_url_query req_perform_iterative resp_body_json last_response req_retry
#' @importFrom dplyr bind_rows mutate tibble distinct select case_when across
#' @importFrom purrr map map2 compact
#' @importFrom tidyr replace_na
#' @importFrom stringr str_extract str_detect str_to_lower
#' @export
#' @examples
#' \dontrun{
#' get_meta_data("2026-03-01", "2026-04-20", granularity = "daily")
#' get_meta_data("2026-04-18", "2026-04-20", granularity = "hourly")
#' }
get_meta_data <- function(start_date,
                          end_date,
                          granularity = c("daily", "hourly"),
                          token = Sys.getenv("META_ACCESS_TOKEN"),
                          ad_account_id = Sys.getenv("META_AD_ACCOUNT_ID")) {
  granularity <- match.arg(granularity)

  # 1. Credentials
  if (token == "" || ad_account_id == "") {
    stop("API Credentials fehlen! Bitte META_ACCESS_TOKEN und META_AD_ACCOUNT_ID in .Renviron setzen.")
  }

  start_date <- as.Date(start_date)
  end_date <- as.Date(end_date)
  if (start_date > end_date) stop("`start_date` liegt nach `end_date`.")

  # 2. Chunking je nach Granularität
  if (granularity == "daily") {
    s_date_seq <- seq(start_date, end_date, by = "30 days")
    e_date_seq <- c(s_date_seq[-1] - 1, end_date)
  } else {
    # Meta erlaubt time_increment=1 NICHT mit Stunden-Breakdowns → Tag für Tag
    s_date_seq <- seq(start_date, end_date, by = "1 day")
    e_date_seq <- s_date_seq
  }

  base_url <- paste0("https://graph.facebook.com/v19.0/", ad_account_id, "/insights")
  message(
    "Starte Meta-Datenabruf [", granularity, "] für ",
    length(s_date_seq), " Chunk(s) (", start_date, " bis ", end_date, ")..."
  )

  # 3. Fetch eines Chunks inkl. Pagination
  fetch_chunk <- function(s_chunk, e_chunk) {
    time_range_json <- sprintf('{"since":"%s","until":"%s"}', s_chunk, e_chunk)

    req <- httr2::request(base_url) |>
      httr2::req_auth_bearer_token(token) |>
      httr2::req_retry(max_tries = 3, max_seconds = 15) |>
      httr2::req_url_query(
        level      = "ad",
        fields     = "date_start,date_stop,account_id,campaign_id,campaign_name,adset_id,adset_name,ad_id,ad_name,spend,impressions,clicks",
        time_range = time_range_json,
        limit      = "100"
      )

    req <- if (granularity == "daily") {
      req |> httr2::req_url_query(time_increment = "1")
    } else {
      req |> httr2::req_url_query(breakdowns = "hourly_stats_aggregated_by_advertiser_time_zone")
    }

    tryCatch(
      {
        resps <- httr2::req_perform_iterative(
          req,
          next_req = function(resp, req) {
            body <- httr2::resp_body_json(resp)
            next_url <- body$paging$`next`
            if (is.null(next_url)) {
              return(NULL)
            }
            httr2::request(next_url) |> httr2::req_auth_bearer_token(token)
          },
          max_reqs = Inf
        )

        all_data <- purrr::map(resps, function(r) {
          body <- httr2::resp_body_json(r)
          if (length(body$data) > 0) {
            purrr::map(body$data, ~ as.data.frame(purrr::compact(.x))) |>
              dplyr::bind_rows()
          } else {
            NULL
          }
        }) |> dplyr::bind_rows()

        dplyr::tibble(all_data)
      },
      error = function(e) {
        err_msg <- e$message
        if (!is.null(httr2::last_response())) {
          meta_err <- try(httr2::resp_body_json(httr2::last_response())$error$message, silent = TRUE)
          if (!inherits(meta_err, "try-error") && !is.null(meta_err)) {
            err_msg <- paste0("HTTP ", httr2::last_response()$status_code, " | Meta sagt: ", meta_err)
          }
        }
        warning("Fehler Zeitraum ", s_chunk, " bis ", e_chunk, ": ", err_msg, call. = FALSE)
        dplyr::tibble()
      }
    )
  }

  # 4. Iteration über alle Chunks
  all_data_list <- purrr::map2(s_date_seq, e_date_seq, function(s, e) {
    message("Rufe Daten ab: ", s, " bis ", e)
    fetch_chunk(s, e)
  })
  all_data <- dplyr::bind_rows(all_data_list)

  if (nrow(all_data) == 0) {
    message("Keine Daten im Gesamtzielraum gefunden.")
    return(dplyr::tibble())
  }

  # 5. Typen anpassen (gemeinsam) — date_stop bleibt absichtlich <chr>,
  #    um exakt dem bestehenden Master-Schema zu entsprechen
  clean_data <- all_data |>
    dplyr::mutate(
      spend       = as.numeric(spend),
      impressions = as.integer(impressions),
      clicks      = as.integer(clicks),
      date_start  = as.Date(date_start)
    )

  # 6. Granularitäts-spezifisches Select / Schema-Anpassung
  if (granularity == "hourly") {
    clean_data <- clean_data |>
      dplyr::mutate(
        hour = stringr::str_extract(hourly_stats_aggregated_by_advertiser_time_zone, "^\\d{2}"),
        hour = as.character(as.integer(hour)),
        # WICHTIG: lowercase, um dem bestehenden meta_hourly_request-Master zu entsprechen
        custom_channel_attribute_meta_ads_creative_type = dplyr::case_when(
          stringr::str_detect(stringr::str_to_lower(ad_name), "video|reel|tiktok|mp4") ~ "video",
          stringr::str_detect(stringr::str_to_lower(ad_name), "image|static|bild|foto|jpg|png") ~ "image",
          stringr::str_detect(stringr::str_to_lower(ad_name), "carousel|karussell") ~ "carousel",
          TRUE ~ "ungrouped"
        )
      ) |>
      dplyr::select(
        date = date_start, hour,
        account_id,
        campaign_id, campaign_name,
        adset_id, adset_name,
        ad_id, ad_name,
        custom_channel_attribute_meta_ads_creative_type,
        spend, impressions, clicks
      )
  } else {
    # daily: date_start bleibt, date_stop als <chr> hinten dran
    clean_data <- clean_data |>
      dplyr::select(
        date_start,
        account_id,
        campaign_id, campaign_name,
        adset_id, adset_name,
        ad_id, ad_name,
        spend, impressions, clicks,
        date_stop
      )
  }

  clean_data <- clean_data |>
    dplyr::distinct() |>
    dplyr::mutate(
      dplyr::across(c(spend, impressions, clicks), ~ tidyr::replace_na(., 0))
    )

  message("Erfolgreich insgesamt ", nrow(clean_data), " Zeilen geladen.")
  clean_data
}
