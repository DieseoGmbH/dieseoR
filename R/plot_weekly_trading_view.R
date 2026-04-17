#' @title Erstelle einen interaktiven Weekly VSA Trading-Chart
#'
#' @description
#' Aggregiert Shopify-Bestelldaten auf Wochenbasis (OHLC & Volumen) und wendet
#' Volume Spread Analysis (VSA) Logik an, um Kerzen entsprechend ihrer Signifikanz
#' einzufärben. Beinhaltet MAs, Crossovers und einen RSI-Oszillator.
#'
#' @param df Dataframe mit Shopify-Rohdaten (benötigt `created_at`, `item_gross_revenue`, `quantity`).
#' @param cleaned_return_data Dataframe mit gesäuberten Retoure Daten
#' @return Ein interaktives `plotly` Objekt (Subplot aus Main-Chart und RSI).
#' @export
#'
#' @importFrom dplyr mutate group_by summarise arrange lag if_else case_when filter
#' @importFrom lubridate as_date floor_date days
#' @importFrom zoo rollmean
#' @importFrom plotly plot_ly add_lines add_segments add_markers layout subplot config
plot_weekly_trading_view <- function(df, cleaned_return_data) {
  df <- df |>
    rename("shopify_order_id" = order_id) |>
    left_join(cleaned_return_data |>
      filter(type == "return") |>
      select(shopify_order_id, type) |> distinct()) |>
    filter(is.na(type)) |>
    filter(financial_status %in% c("paid", "partially_paid"))
  # 1. DATENAUFBEREITUNG & INDIKATOREN
  weekly_data <- df |>
    dplyr::mutate(date = lubridate::as_date(created_at)) |>
    dplyr::group_by(date) |>
    dplyr::summarise(
      daily_revenue = sum(item_gross_revenue, na.rm = TRUE),
      daily_quantity = sum(quantity, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::arrange(date) |>
    dplyr::mutate(week_start = lubridate::floor_date(date, unit = "week", week_start = 1)) |>
    dplyr::group_by(week_start) |>
    dplyr::summarise(
      high = max(daily_revenue, na.rm = TRUE),
      low = min(daily_revenue, na.rm = TRUE),
      close = dplyr::last(daily_revenue),
      volume = sum(daily_quantity, na.rm = TRUE),
      .groups = "drop"
    ) |>
    dplyr::mutate(
      open = dplyr::lag(close),
      open = dplyr::if_else(is.na(open), dplyr::first(low), open),
      bullish = close >= open,
      vol_sma4 = zoo::rollmean(volume, k = 4, fill = NA, align = "right"),
      fill_color = dplyr::case_when(
        bullish & volume >= 2.0 * vol_sma4 ~ "green",
        bullish & volume >= 1.5 * vol_sma4 ~ "turquoise",
        bullish ~ "yellow",
        !bullish & volume <= 0.5 * vol_sma4 ~ "red",
        !bullish & volume <= 0.75 * vol_sma4 ~ "pink",
        !bullish ~ "grey",
        TRUE ~ "grey"
      ),
      sma_fast = zoo::rollmean(close, k = 4, fill = NA, align = "right"),
      sma_slow = zoo::rollmean(close, k = 12, fill = NA, align = "right"),

      # --- KORRIGIERTE CROSSOVER LOGIK ---
      # Crossover passiert exakt in der Woche, in der die Fast-Linie die Slow-Linie durchbricht.
      cross_up = dplyr::lag(sma_fast) < dplyr::lag(sma_slow) & sma_fast >= sma_slow,
      cross_dn = dplyr::lag(sma_fast) > dplyr::lag(sma_slow) & sma_fast <= sma_slow,
      is_crossover = !is.na(cross_up) & !is.na(cross_dn) & (cross_up | cross_dn),
      change = close - dplyr::lag(close),
      gain = dplyr::if_else(change > 0, change, 0),
      loss = dplyr::if_else(change < 0, abs(change), 0),
      avg_gain = zoo::rollmean(gain, k = 14, fill = NA, align = "right"),
      avg_loss = zoo::rollmean(loss, k = 14, fill = NA, align = "right"),
      rs = avg_gain / dplyr::if_else(avg_loss == 0, 1e-10, avg_loss),
      rsi = 100 - (100 / (1 + rs)),
      hover_text = sprintf(
        "<b>Woche: %s</b><br>Open: %.2f €<br>High: %.2f €<br>Low: %.2f €<br>Close: %.2f €<br>Volumen: %d Stück",
        format(week_start, "%d.%m.%Y"), open, high, low, close, volume
      )
    )

  # 2. HAUPTCHART (PLOTLY)
  # Wir starten mit einem unsichtbaren Dummy-Trace, der unser Master-Schalter in der Legende wird.
  p_main <- plotly::plot_ly() |>
    plotly::add_lines(
      x = c(weekly_data$week_start[1], weekly_data$week_start[1]),
      y = c(weekly_data$close[1], weekly_data$close[1]),
      line = list(color = "rgba(0,0,0,0)"), # Komplett transparent
      name = "Candles", legendgroup = "candles", showlegend = TRUE, hoverinfo = "none"
    )

  # Farben loop (Bulletproof für Plotly)
  colors_present <- unique(weekly_data$fill_color)
  for (color_iter in colors_present) {
    p_main <- local({
      current_color <- color_iter
      sub_df <- weekly_data[weekly_data$fill_color == current_color, ]

      p_main |>
        plotly::add_segments(
          data = sub_df,
          x = ~week_start, xend = ~week_start, y = ~low, yend = ~high,
          line = list(color = current_color, width = 1.5), opacity = 0.4,
          legendgroup = "candles", showlegend = FALSE, hoverinfo = "none"
        ) |>
        plotly::add_segments(
          data = sub_df,
          x = ~week_start, xend = ~week_start, y = ~open, yend = ~close,
          line = list(color = current_color, width = 6),
          legendgroup = "candles", showlegend = FALSE,
          text = ~hover_text, hoverinfo = "text"
        )
    })
  }

  # MAs und Crossover Kreuze hinzufügen
  p_main <- p_main |>
    plotly::add_lines(
      data = weekly_data, x = ~week_start, y = ~sma_fast,
      name = "Fast SMA (4W)", line = list(color = "orange", width = 2), hoverinfo = "y"
    ) |>
    plotly::add_lines(
      data = weekly_data, x = ~week_start, y = ~sma_slow,
      name = "Slow SMA (12W)", line = list(color = "steelblue", width = 2), hoverinfo = "y"
    ) |>
    plotly::add_markers(
      data = dplyr::filter(weekly_data, is_crossover),
      x = ~week_start, y = ~sma_fast,
      name = "Crossovers", legendgroup = "crossovers",
      marker = list(symbol = "cross", color = "white", size = 8, line = list(width = 2)),
      text = "MA Crossover!", hoverinfo = "text"
    ) |>
    plotly::layout(
      yaxis = list(title = "Wochenumsatz", tickformat = ",.2f", ticksuffix = " €")
    )

  # 3. RSI CHART
  p_rsi <- plotly::plot_ly(data = weekly_data, x = ~week_start) |>
    plotly::add_lines(
      y = ~rsi, name = "RSI (14)",
      line = list(color = "#7b1fa2", width = 2),
      text = ~ paste("RSI:", round(rsi, 1)), hoverinfo = "text"
    ) |>
    plotly::layout(
      yaxis = list(title = "RSI (14)", range = c(0, 100), tickvals = c(30, 50, 70)),
      shapes = list(
        list(
          type = "rect", x0 = 0, x1 = 1, xref = "paper", y0 = 30, y1 = 70, yref = "y",
          fillcolor = "#787b86", opacity = 0.1, line = list(width = 0)
        ),
        list(
          type = "line", x0 = 0, x1 = 1, xref = "paper", y0 = 70, y1 = 70, yref = "y",
          line = list(color = "#787b86", dash = "dash")
        ),
        list(
          type = "line", x0 = 0, x1 = 1, xref = "paper", y0 = 30, y1 = 30, yref = "y",
          line = list(color = "#787b86", dash = "dash")
        )
      )
    )

  # 4. DASHBOARD ZUSAMMENFASSUNG
  interactive_chart <- plotly::subplot(
    p_main, p_rsi,
    nrows = 2, heights = c(0.75, 0.25), shareX = TRUE, titleY = TRUE
  ) |>
    plotly::layout(
      title = list(text = "Pammys Weekly Market Structure (VSA & Crossovers)", font = list(color = "white")),
      plot_bgcolor = "#131722",
      paper_bgcolor = "#131722",
      font = list(color = "#d1d4dc"),
      hovermode = "x unified",
      legend = list(orientation = "h", x = 0, y = 1.05)
    ) |>
    plotly::config(displayModeBar = FALSE)

  return(interactive_chart)
}
