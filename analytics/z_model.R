library(tidyverse)
library(duckdb)
library(tidymodels)

# tbl(con, "combined_data") |>
#   mutate(y = y - 26.65) |>
#   filter(player_side == 'Offense') |>
#   inner_join(tbl(con, "play_starts")) |>
#   filter(frame_id < frame_start) |>
#   group_by(game_id, play_id, frame_id) |>
#   mutate(
#     num_offense_outside = sum()
#   )

get_model_df <- function() {
  pair_features <- tbl(con, "pair_features")
  distances <- tbl(con, "distances")
  combined_data <- tbl(con, "combined_data")
  ball_is_in_air_df <- combined_data |>
    filter(dataset == 'Y') |>
    summarise(ball_in_air = min(frame_id), .by = c(game_id, play_id))

  sup_data <- tbl(con, "supplementary_data") |>
    select(
      game_id,
      play_id,
      team_coverage_type,
      distance_to_goal,
      yards_to_go,
      route_of_targeted_receiver,
      play_action,
      team_coverage_man_zone
    )

  pair_features |>
    group_by(game_id, play_id, nfl_id, frame_id) |>
    mutate(pred_coverage = pred_coverage / sum(pred_coverage)) |>
    ungroup() |>
    inner_join(
      distances |>
        filter(player_to_predict, player_to_predict_def) |>
        distinct(game_id, play_id, nfl_id, nfl_id_def)
    ) |>
    left_join(
      combined_data |> distinct(nfl_id, player_position)
    ) |>
    left_join(
      combined_data |> distinct(nfl_id, player_position)
    ) |>
    left_join(
      sup_data
    ) |>
    left_join(
      distances |> distinct(nfl_id_def, player_position_def)
    ) |>
    left_join(
      get_play_starts()
    ) |>
    left_join(
      ball_is_in_air_df
    ) |>
    filter(frame_id > frame_start + 5) |>
    group_by(nfl_id, game_id, play_id, nfl_id_def) |>
    dbplyr::window_order(frame_id) |>
    mutate(
      distance_diff = distance - lag(distance),
      diff_turn_angle_mag = diff_turn_angle - lag(diff_turn_angle),
      z_turn = (diff_turn_angle - median(diff_turn_angle)) /
        sd(diff_turn_angle),
      route_depth = x - lag(x),
      route_depth = ifelse(is.na(route_depth), 0, route_depth),
      route_depth = cumsum(route_depth),
    ) |>
    ungroup() |>
    group_by(game_id, play_id, nfl_id, nfl_id_def) |>
    mutate(
      mean_pred_coverage = mean(pred_coverage),
      max_pred_coverage = max(pred_coverage)
    ) |>
    ungroup() |>
    #filter(frame_id > 10) |>
    mutate(
      win = as.numeric(abs(z_turn) > 2.5),
      win = ifelse(is.na(win), 0, win)
    )
}
