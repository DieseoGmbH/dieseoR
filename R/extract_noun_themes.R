#' @title Extract core themes with POS tagging (removing adjectives)
#'
#' @description
#' Annotates text to identify grammatical structures and removes Adjectives (ADJ)
#' to focus purely on topic-driven nouns and entities.
#'
#' @param df Dataframe with Trustpilot data.
#' @param model_path Path to the downloaded udpipe model (e.g., "german-gsd-ud-2.5-191206.udpipe").
#' @param top_n Number of top terms to return.
#'
#' @importFrom dplyr filter mutate select count group_by slice_max ungroup arrange desc left_join
#' @importFrom udpipe udpipe_load_model udpipe_annotate
#' @importFrom rlang .data
#' @export
extract_noun_themes <- function(df, model_path, top_n = 10) {
  # 1. Vorbereitung (wie gehabt)
  df_prep <- df |>
    dplyr::filter(.data$language == "de") |>
    tidyr::drop_na(stars) |>
    dplyr::mutate(
      rating_group = base::ifelse(.data$stars <= 3, "Kritisch (1-3)", "Positiv (4-5)"),
      full_text = stringr::str_c(.data$title, " ", .data$text)
    )

  # 2. Modell laden
  ud_model <- udpipe::udpipe_load_model(file = model_path)

  # 3. Text annotieren (Das ist der rechenintensive Schritt!)
  # udpipe liefert einen detaillierten Dataframe mit Wörtern und ihren POS-Tags zurück
  annotation <- udpipe::udpipe_annotate(
    ud_model,
    x = df_prep$full_text,
    doc_id = df_prep$id
  )
  df_annotated <- base::as.data.frame(annotation)

  # 4. Filtern: Adjektive rauswerfen!
  # Wir können hier sogar noch restriktiver sein und z.B. NUR Nomen (NOUN) behalten.
  tokens_clean <- df_annotated |>
    # upos = Universal Part of Speech. "ADJ" sind Adjektive.
    dplyr::filter(.data$upos != "ADJ") |>
    # Optionaler Profi-Tipp: Wenn du WIRKLICH nur Themen willst, behalte nur Nomen:
    dplyr::filter(.data$upos %in% c("NOUN", "PROPN")) |>
    dplyr::rename(word = lemma) |> # lemma ist die Grundform (z.B. "Schuhen" -> "Schuh")
    dplyr::mutate(word = base::tolower(.data$word)) |>
    # Eigene Stopwörter/Pammys-spezifisches rauswerfen
    dplyr::filter(!.data$word %in% c("schuh", "pammys", "pummys", "frist", "juli", "kontaktformular"))

  # Um die TF-IDF Metrik zu berechnen, joinen wir die rating_group wieder an
  # (da udpipe nur die doc_id = id behalten hat)
  tokens_with_group <- tokens_clean |>
    dplyr::left_join(df_prep |> dplyr::select(.data$id, .data$rating_group),
      by = c("doc_id" = "id")
    )

  # 5. TF-IDF Berechnung (wie vorher)
  theme_scores <- tokens_with_group |>
    dplyr::count(.data$rating_group, .data$word, sort = TRUE) |>
    tidytext::bind_tf_idf(term = .data$word, document = .data$rating_group, n = .data$n)

  top_themes <- theme_scores |>
    dplyr::group_by(.data$rating_group) |>
    dplyr::slice_max(order_by = .data$tf_idf, n = top_n, with_ties = FALSE) |>
    dplyr::ungroup() |>
    dplyr::arrange(.data$rating_group, dplyr::desc(.data$tf_idf))

  return(top_themes)
}
