library(tidyverse)
library(duckdb)
source('analytics/prep_data.R')
source('analytics/distance_data.R')
# source('analytics/coverage_assignments2.R')
source('analytics/helpers.R')
con <- dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

# prep_data(con, load_data = TRUE)
# load_distance_data(con)
# load_coverage_data(con, run_gmm = FALSE)

# --

tbl(con, "coverage_predictions") |>
  filter(frame_id > 10) |>
  group_by(game_id, play_id, nfl_id_def) |>

  dbplyr::window_order(frame_id) |>
  dbplyr::window_frame(from = -2, to = 2) |>
  mutate(
    pred_coverage = mean(.pred_1)
  ) |>
  ungroup() |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  ungroup() |>
  compute(name = "coverage_predictions_stg_2", overwrite = TRUE)


df <- tbl(con, "coverage_predictions_stg_2") |>
  mutate(
    diff_turn_angle = atan2(
      sin(turn_angle - turn_angle_def),
      cos(turn_angle - turn_angle_def)
    ),
    diff_turn_angle = diff_turn_angle * pred_coverage,
    z_turn_pop = (diff_turn_angle - mean(diff_turn_angle)) / sd(diff_turn_angle)
  ) |>
  ungroup() |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    z_turn_play = (diff_turn_angle - mean(diff_turn_angle, na.rm = TRUE)) /
      sd(diff_turn_angle, na.rm = TRUE),
    accel_change = (a - lag(a)),
    accel_change_def = (a_def - lag(a_def)),
    accel_diff = accel_change - accel_change_def,
    cov_max = max(pred_coverage)
  ) |>
  ungroup()

df |>
  #inner_join(df |> distinct(game_id, play_id) |> slice_sample(n = 1)) |>
  play_filter(get_common_plays()) |>
  collect() |>
  select(
    game_id,
    frame_id,
    player_name,
    player_name_def,
    cov_max,
    .pred_1,
    pred_coverage,
    diff_turn_angle,
    turn_angle,
    turn_angle_def,
    a,
    a_def,
    accel_change,
    accel_change_def,
    accel_diff,
    z_turn_pop,
    z_turn_play
  ) |>
  pivot_longer(cov_max:z_turn_play) |>
  filter(str_detect(name, "pred") | name == 'cov_max') |>
  #filter( name %in% c('a', 'a_def')) |>
  #filter(frame_id > 10) |>
  ggplot(aes(frame_id, value, color = name)) +
  geom_line() +
  #geom_hline(yintercept = c(-2, 2), linetype = 'dashed') +
  facet_wrap(
    ~ paste(player_name, player_name_def, sep = '\n'),
    scales = 'free'
  )


dt <- 0.1 # 10 Hz tracking
contest_dist <- 1.0

df |>
  group_by(
    game_id,
    play_id,
    nfl_id,
    player_name,
    nfl_id_def,
    player_name_def
  ) |>
  #arrange(frame_id, .by_group = TRUE) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    sep_trigger = abs(z_turn_pop) > 2.5 &
      abs(z_turn_play) > 2 &
      lag(z_turn_play) < 2,
    sep_id = cumsum(sep_trigger)
  ) |>
  filter(sep_id > 0) |>
  collect() |>
  group_by(
    game_id,
    play_id,
    nfl_id,
    nfl_id_def,
    player_name,
    player_name_def,
    player_position,
    player_position_def,
    sep_id
  ) |>
  mutate(
    reverse_cummax = rev(cummax(rev(distance))),
    delta_recovery = reverse_cummax - distance,
    impact_contrib = delta_recovery * dt,
  ) |>
  summarise(
    max_distance = max(distance),
    recovery_auc = sum(impact_contrib, na.rm = TRUE),
    peak_recovery_gap = max(delta_recovery, na.rm = TRUE),
    recovery_frames = sum(delta_recovery > 0, na.rm = TRUE),
    .groups = "drop"
  ) |>
  mutate(
    ESS = max_distance + peak_recovery_gap,
    ISI = max_distance + recovery_auc,
    SEP_DELTA = ISI - max_distance
  ) -> .df

.df
