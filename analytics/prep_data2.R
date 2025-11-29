library(tidyverse)
library(tidypolars)


get_supplementary_data <- function() {
  tidypolars::read_csv_polars(
    here::here('data', 'supplementary_data.csv'),
    infer_schema_length = NULL
  ) |>
    mutate(
      distance_to_goal = if_else(
        possession_team == yardline_side,
        100 - yardline_number,
        yardline_number
      )
    )
}


get_player_data <- function() {
  scan_csv_polars('data/train/input*.csv') |>
    select(player_name, player_height, player_weight) |>
    distinct() |>
    as_polars_df() |>
    as_tibble() |>
    mutate(
      player_height = stringr::str_split(player_height, '-'),
      player_height = map(player_height, as.numeric),
      player_height = map_dbl(.x = player_height, ~ .x[1] * 12 + .x[2]),
      player_height_z = (player_height - mean(player_height)) /
        sd(player_height),
      player_weight_z = (player_weight - mean(player_weight)) /
        sd(player_weight)
    )
}


read_csv_polars('data/train/input*.csv') -> df
read_csv_polars('data/train/output*.csv') -> df_output
get_supplementary_data() -> df_sup

df_sup


df |>
  left_join(df_sup |> select(game_id, play_id, distance_to_goal)) |>
  select(game_id, play_id, distance_to_goal) |>
  distinct()


add_tracking_features <- function(.data) {
  .data |>
    mutate(
      dx = lead(x_rel) - x_rel,
      dy = lead(y) - y,
      dt = 1 / 10,
      vx = dx / dt,
      vy = dy / dt,
      s = sqrt(vx^2 + vy^2),
      s_mph = s * (3600 / 1760),
      ax = (lead(vx) - vx) / dt,
      ay = (lead(vy) - vy) / dt,
      accel = sqrt(ax^2 + ay^2),
      dir = atan2(vy, vx) * 180 / pi,
      dir = (dir + 360) %% 360,
      x_end_rel = s * cos((90 - dir) * pi / 180) + x_rel,
      y_end = s * sin((90 - dir) * pi / 180) + y,
      turn_angle = atan2(
        lead(y) - y,
        lead(x_rel) - x_rel
      ) -
        atan2(
          y - dplyr::lag(y),
          x_rel - dplyr::lag(x_rel)
        ),
      turn_angle = ifelse(
        turn_angle >= pi,
        turn_angle - 2 * pi,
        ifelse(turn_angle <= -pi, 2 * pi + turn_angle, turn_angle)
      ),
      turn_angle = ifelse(
        turn_angle > 3.14 | turn_angle < -3.14,
        0,
        turn_angle
      ),
      prev_angle = dplyr::lag(turn_angle)
    ) |>
    ungroup()
}


df |>
  left_join(df_sup |> select(game_id, play_id, distance_to_goal)) |>
  standardize_tracking_directions() |>
  group_by(game_id, play_id, nfl_id) |>
  arrange(frame_id, .by_group = TRUE) |>
  add_tracking_features()
