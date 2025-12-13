library(tidyverse)
library(duckdb)
# source('analytics/prep_data.R')
# source('analytics/distance_data.R')
# source('analytics/coverage_assignments2.R')
source('analytics/helpers.R')
con <- dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

# prep_data(con, load_data = FALSE)
# load_distance_data(con)
# load_coverage_data(con, run_gmm = FALSE)

df <- tbl(con, "coverage_predictions") |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    diff_turn_angle = atan2(
      sin(turn_angle - turn_angle_def),
      cos(turn_angle - turn_angle_def)
    ),
    z_turn = (diff_turn_angle - mean(diff_turn_angle)) / sd(diff_turn_angle)
  ) |>
  dbplyr::window_frame(-3) |>
  mutate(
    pred_coverage = mean(.pred_1)
  ) |>
  ungroup()

# --
get_model_df() |>
  mutate(z_turn = z_turn * pred_coverage) |>
  collect() |>
  group_by(game_id, play_id, player_name, player_name_def) %>%
  arrange(frame_id, .by_group = TRUE) %>%
  filter(!(frame_id == min(frame_id) & z_turn == max(z_turn))) |>
  mutate(
    # post pred_coverage calculations
    turn_angle_mag = abs(diff_turn_angle),
    angle_mag = abs(angle_with_def_diff),
    # id
    sep = abs(z_turn) > 2,
    sep_id = cumsum(sep),
    distance_diff = ifelse(is.na(distance_diff), 0, 1),
    greatest_distance_increasing = max(
      ifelse(distance_diff > 0, frame_id, 0),
      na.rm = TRUE
    )
    #reverse_cummax = rev(cummax(rev(distance))),
  ) |>
  filter(sep_id > 0) |>
  #inner_join(play |> select(game_id, play_id)) |>
  ungroup() |>
  #filter(player_name %in% c('Mike Evans')) |>
  #ggplot(aes(frame_id)) +geom_point(aes(y=distance_increasing))
  group_by(game_id, play_id, player_name, player_name_def, sep_id) %>%
  summarise(
    route_depth = max(
      ifelse(frame_id < ball_in_air, route_depth, NA),
      na.rm = TRUE
    ),
    ball_in_air = min(ball_in_air, na.rm = TRUE),
    break_frame = first(frame_id),
    break_distance = first(distance),
    break_z_turn = first(z_turn),
    break_angle = first(angle_with_def),
    min_distance = min(distance, na.rm = TRUE),
    max_distance = max(distance, na.rm = TRUE),
    max_separation_gained = max_distance - break_distance,
    peak_separation_frame = frame_id[which.max(distance)],

    t_max_sep = max(greatest_distance_increasing, na.rm = TRUE),
    #last_increasing_frame = max(frame_id[distance_increasing], na.rm = TRUE)

    defender_closed_gap = any(
      distance < break_distance & frame_id > peak_separation_frame
    ),

    # If defender recovered, find when
    recovery_frame = if_else(
      defender_closed_gap,
      frame_id[which(
        distance < break_distance & frame_id > peak_separation_frame
      )[1]],
      NA_integer_
    ),

    frames_until_recovery = if_else(
      defender_closed_gap,
      recovery_frame - peak_separation_frame,
      NA_integer_
    ),

    max_sep = distance[
      frame_id == max(greatest_distance_increasing, na.rm = TRUE)
    ],
    max_angle_frame = frame_id[which.max(abs(angle_with_def_diff))],
    cov_mean = mean(pred_coverage),
    cov_peak = pred_coverage[which.max(distance_diff)],
    .groups = 'drop'
  ) |>
  #filter(cov_mean > 0.5, cov_peak > 0.5, break_distance < 10) |>
  select(-starts_with('cov')) |>
  mutate(
    frames_created = t_max_sep - break_frame
  ) |>
  arrange(-max_sep) -> df

df |>
  ggplot(aes(max_sep)) +
  geom_histogram()

df |>
  ggplot(aes(route_depth)) +
  geom_histogram()

df |>
  # group_by(player_name) |>
  # summarise(
  #   seconds_created = mean(frames_created) / 10,
  #   separation_created = mean(max_sep),
  #   n = n()
  # ) |>
  rename(seconds_created = frames_created, separation_created = max_sep) |>
  ggplot(aes(seconds_created / 10)) +
  geom_histogram()
#ggrepel::geom_text_repel(aes(label = player_name), size = 3)

df |>
  filter(route_depth > 7) |>
  #filter(break_frame > ball_in_air) |>
  slice_sample(n = 5) -> tmp
#inner_join(get_common_plays()) |> glimpse()

tmp |>
  select(
    player_name,
    player_name_def,
    sep_id,
    defender_closed_gap,
    frames_until_recovery,
    max_sep,
    frames_created,
    break_distance,
    min_distance,
    max_distance
  )

#' could look at removing max_sep = max_distance

tbl(con, "pair_features") |>
  group_by(game_id, play_id, nfl_id, frame_id) |>
  mutate(
    pred_coverage_o = pred_coverage / sum(pred_coverage),
  ) |>
  ungroup() |>
  group_by(game_id, play_id, nfl_id_def, frame_id) |>
  mutate(
    pred_coverage_d = pred_coverage / sum(pred_coverage),
  ) |>
  ungroup() |>
  play_filter(
    get_common_plays() |> select(-player_name)
    #tmp |> distinct(game_id, play_id, player_name_def, player_name)
  ) |>
  collect() -> tmp

plays <- tmp |> distinct(game_id, play_id)


tmp |>
  inner_join(plays[9, ]) |>
  select(
    frame_id,
    player_name,
    player_name_def,
    pred_coverage,
    pred_coverage_o,
    pred_coverage_d
  ) |>
  pivot_longer(pred_coverage:pred_coverage_d) |>
  filter(name == 'pred_coverage_d') |>
  ggplot(aes(frame_id, value, color = player_name)) +
  geom_line() +
  facet_wrap(
    ~player_name_def, #~ paste(player_name, player_name_def, sep = '\n'),
    scales = 'free_y'
  )


tmp |>
  inner_join(plays[9, ]) |>
  ggplot(aes(frame_id, dx_pair, color = player_name_def)) +
  geom_line() +
  geom_line(aes(y = angle_with_def)) +
  facet_wrap(
    ~player_name, #~ paste(player_name, player_name_def, sep = '\n'),
    scales = 'free_y'
  )


get_model_df() |>
  play_filter(
    get_common_plays()
    #tmp |> distinct(game_id, play_id, player_name_def, player_name)
  ) |>
  mutate(z_turn = z_turn * pred_coverage) |>
  collect() |>
  ggplot(aes(frame_id)) +
  # geom_line(aes(y = pred_coverage, color = 'pred'), nudge_y = 0.1) +
  # geom_line(aes(y = pred_coverage_1, color = 'pred grp D'), alpha = 0.5) +
  # geom_line(aes(y = pred_coverage_2, color = 'pred grp O')) +
  #geom_line(aes(y = pred_coverage)) +
  geom_point(aes(y = z_turn)) +
  geom_point(aes(y = 0, x = ball_in_air, color = 'red')) +
  geom_hline(yintercept = 2.5, linetype = 'dashed') +
  geom_hline(yintercept = -2.5, linetype = 'dashed') +
  facet_wrap(
    ~ paste(player_name, player_name_def, sep = '\n'),
    scales = 'free_y'
  )


tbl(con, "supplementary_data") |>
  play_filter(tmp) |>
  select(possession_team, defensive_team, week, quarter, down, play_description)
