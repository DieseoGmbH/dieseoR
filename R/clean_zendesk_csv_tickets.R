#' Clean and Map Zendesk Ticket Data
#'
#' Cleans a Zendesk tickets data.frame by removing unnecessary columns,
#' mapping assignee and group IDs to names, and optionally
#' saving the cleaned data.
#'
#' @param data A data.frame containing Zendesk ticket data.
#' @param datadir Character. Directory path where the cleaned
#' file should be saved.
#' @param save Logical. Whether to save the cleaned data as an RDS file.
#' Default is \code{TRUE}.
#' @param save_path Character. Full file path for saving the cleaned data.
#' Default is \code{file.path(datadir, "zendesk_csv_tickets_cleaned.rds")}.
#'
#' @return The cleaned data.frame with mapped assignee and group names.
#' @export
clean_zendesk_csv_tickets <- function(
  data,
  save = TRUE,
  save_path = file.path(datadir, "zendesk/zendesk_csv_tickets_cleaned.rds") # nolint
) {
  id_name_map <- c(
    "8340805378845" = "stephanie",
    "8343002140829" = "laura",
    "17074770479005" = "anja",
    "18604712913053" = "erleta",
    "23356015505821" = "jessica",
    "26311427127837" = "altin",
    "27416907139357" = "AI",
    "30531051544093" = "anastasia",
    "22859146419357" = "abulena",
    "13506489916829" = "johanna",
    "12207192040477" = "nina",
    "5832429026205" = "pammys_support"
  )

  group_id_name_map <- c(
    "22911784253981" = "albulena_lena",
    "26311494547357" = "altin_alina",
    "30531277961245" = "anastasia_kyla",
    "17119690258461" = "anja_anna",
    "23777047107485" = "e_mail",
    "18604992762525" = "erleta_emily",
    "23356326562589" = "jessica_diana",
    "13531713501725" = "johanna_lina",
    "8342692463389"  = "laura_maya",
    "30282063790237" = "lea2",
    "8343306565533"  = "pammys_support2",
    "8342691778717"  = "stephanie_sophia",
    "5832422206621"  = "support",
    "32264914166173" = "support_leitung",
    "29004950157597" = "n8n_automation",
    "21323973965213" = "0smmak",
    "21449217355933" = "bestselling_lieferadresse",
    "21453222774173" = "oos",
    "21479307025565" = "schadensanzeige_nachforschungsantrag",
    "21479331372061" = "stornierung",
    "21453418350749" = "retourenportal",
    "21449287544349" = "falschlieferung",
    "21449298415901" = "reklamation",
    "21449322420381" = "umtausch",
    "21449278255901" = "rueckgabe_rechnung",
    "21453546452893" = "sonstiges",
    "21453280444701" = "hohe_prio_feedback",
    "21452898440605" = "meta",
    "21594391517981" = "paypal_klarna"
  )

  replace_ids_partial <- function(x) {
    x_chr <- as.character(x)
    for (pat in names(id_name_map)) {
      x_chr <- dplyr::if_else(
        !is.na(x_chr) & stringr::str_detect(x_chr, paste0("^", pat)),
        id_name_map[[pat]],
        x_chr
      )
    }
    x_chr[is.na(x_chr)] <- NA_character_
    x_chr
  }

  replace_groups <- function(x) {
    x_chr <- as.character(x)
    x_chr <- dplyr::if_else(
      !is.na(x_chr) & x_chr %in% names(group_id_name_map),
      group_id_name_map[x_chr],
      x_chr
    )
    x_chr[is.na(x_chr)] <- NA_character_
    x_chr
  }

  cleaned <- data |>
    clean_master() |> # nolint
    dplyr::select(-c(
      organization_id, problem_id, brand_id, due_at, # nolint
      satisfaction_score, satisfaction_reason, # nolint
      inbound_sharing_agreement, outbound_sharing_agreement, # nolint
      type, priority, brand, form, custom_status_id, external_id # nolint
    )) |>
    dplyr::mutate(
      assignee_id = replace_ids_partial(assignee_id), # nolint
      group_id = replace_groups(group_id) # nolint
    ) |>
    dplyr::distinct()

  if (save) {
    saveRDS(cleaned, save_path)
  }

  cleaned
}
