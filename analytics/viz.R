library(gganimate)
library(tidyverse)
source("https://raw.githubusercontent.com/mlfurman3/gg_field/main/gg_field.R")
teams <- nflreadr::load_teams()
sup_data <- tbl(con, "supplementary_data") |> collect()

plot_df <- df$x |>
  select(game_id, play_id, nfl_id, frame_id, x_rel, y) |>
  mutate(
    dataset = 'X'
  ) |>
  bind_rows(
    df$y |>
      select(game_id, play_id, nfl_id, frame_id, x_rel, y) |>
      mutate(dataset = 'Y')
  ) |>
  group_by(game_id, play_id, nfl_id) |>
  arrange(dataset, frame_id, .by_group = TRUE) |>
  mutate(frame_id2 = row_number()) |>
  ungroup() |>
  inner_join(
    df$x |> distinct(game_id, play_id, ball_land_x_rel, ball_land_y)
  ) |>
  inner_join(
    df$x |> distinct(game_id, play_id, nfl_id, player_side, player_position)
  ) |>
  left_join(
    sup_data |>
      select(
        game_id,
        play_id,
        play_description,
        possession_team,
        defensive_team
      ),
    by = c('game_id', 'play_id')
  ) |>
  left_join(
    teams |>
      select(possession_team = team_abbr, team_color, team_color2)
  ) |>
  left_join(
    teams |>
      select(defensive_team = team_abbr, team_color, team_color2),
    by = c('defensive_team')
  ) |>
  mutate(
    team_color = case_when(
      player_side == 'Offense' ~ team_color.x,
      player_side == 'Defense' ~ team_color.y,
      TRUE ~ NA
    ),
    team_color2 = case_when(
      player_side == 'Offense' ~ team_color2.x,
      player_side == 'Defense' ~ team_color2.y,
      TRUE ~ NA
    )
  ) |>
  select(-ends_with('.y'), -ends_with('.x'))

watch_film <- function(plot_df, play) {
  d <- plot_df |>
    mutate(x_rel = x_rel + 15) |>
    inner_join(play) |>
    mutate(
      cols_fill = if_else(is.na(team_color), "#663300", team_color),
      cols_col = if_else(is.na(team_color), "#663300", team_color2),
      size_vals = if_else(is.na(team_color), 4, 6),
      shape_vals = if_else(is.na(team_color), 16, 22)
    )

  plot_title <- d$play_description |> str_sub(8, 60)
  nFrames <- max(d$frame_id2)

  anim <- ggplot() +
    gg_field(
      yardmin = 10,
      yardmax = 50,
      field_color = "white",
      line_color = "black",
      sideline_color = 'white',
      endzone_color = "white"
    ) +
    theme(
      panel.background = element_rect(
        fill = "white",
        color = "white"
      ),
      panel.grid = element_blank()
    ) +
    # setting size and color parameters
    geom_point(
      data = d,
      aes(
        x_rel,
        y,
        shape = shape_vals,
        fill = cols_fill,
        group = nfl_id,
        size = size_vals,
        color = cols_col
      )
    ) +
    #geom_segment(aes(x = x, y = y, xend = x_end, yend = y_end), data = d) +
    geom_text(
      data = d,
      aes(x = x_rel, y = y, label = player_position),
      colour = "white",
      vjust = 0.36,
      size = 3.5
    ) +
    scale_size_identity(guide = FALSE) +
    scale_shape_identity(guide = FALSE) +
    scale_fill_identity(guide = FALSE) +
    scale_colour_identity(guide = FALSE) +
    labs(title = plot_title) +
    transition_time(frame_id2) +
    ease_aes("linear") +
    NULL

  anim_save(
    glue::glue("plays/{unique(plot_title)}.gif"),
    animate(anim, width = 720, height = 440, fps = 10, nframe = nFrames)
  )
}


watch_film(plot_df, play)
