library(tidyverse)
library(duckdb)
source('analytics/prep_data.R')
source('analytics/distance_data.R')
source('analytics/coverage_assignments.R')
con <- dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

prep_data(con, load_data = FALSE)
load_distance_data(con)
load_coverage_data(con, run_gmm = FALSE)

play_filter <- function(.data, play) {
  dbWriteTable(con, "play", play, overwrite = TRUE)

  inner_join(.data, tbl(con, "play"))
}


play <- tibble(
  game_id = c(2023092408),
  play_id = c(1574),
) # trent Sherfield


play <- tibble(
  game_id = c(
    2023092408,
    2023091706,
    2023091710,
    2023101506,
    2023092406,
    2023091706,
    2023090700
  ),
  play_id = c(1574, 2583, 3659, 3367, 2741, 3512, 361),
  player_name = c(
    "Trent Sherfield",
    'Mike Evans',
    'Garrett Wilson',
    'Josh Downs',
    'Mike Williams',
    "Chase Claypool",
    "Marvin Jones"
  )
)


plays <- tbl(con, "pair_features") |> distinct(game_id, play_id, nfl_id)


get_model_df <- function() {
  tbl(con, "pair_features") |>
    inner_join(
      tbl(con, "distances") |>
        filter(player_to_predict, player_to_predict_def) |>
        distinct(game_id, play_id, nfl_id, nfl_id_def)
    ) |>
    left_join(
      tbl(con, "combined_data") |> distinct(nfl_id, player_position)
    ) |>
    #mutate(diff_turn_angle = ifelse(s_def < 1.5, NA, diff_turn_angle)) |>
    filter(frame_id >= 10) |>
    group_by(nfl_id, game_id, play_id, nfl_id_def) |>
    dbplyr::window_order(frame_id) |>
    mutate(
      diff_turn_angle = diff_turn_angle * pred_coverage,
      diff_turn_angle = ifelse(frame_id == first(frame_id), 0, diff_turn_angle),
      angle_with_def = angle_with_def * pred_coverage,
      angle_with_def_diff = angle_with_def - lag(angle_with_def),
      angle_with_def_diff_mag = angle_with_def_diff - lag(angle_with_def_diff),
      #angle_with_def_diff_mag = angle_with_def_diff_mag * pred_coverage,
      distance_diff = distance - lag(distance),
      diff_turn_angle_mag = diff_turn_angle - lag(diff_turn_angle),
      z_turn = (diff_turn_angle - mean(diff_turn_angle)) / sd(diff_turn_angle),
      z_turn = z_turn * pred_coverage,
      z_angle = (angle_with_def_diff - mean(angle_with_def_diff)) /
        sd(angle_with_def_diff)
    ) |>
    ungroup()
}

get_model_df() |>
  #filter(game_id == 2023090700, play_id == 101) |>
  play_filter(play) |>
  select(
    frame_id,
    player_name,
    player_name_def,
    #diff_turn_angle,
    #angle_with_def,
    angle_with_def_diff,
    #angle_with_def_diff_mag,
    #s_def,
    distance_diff,
    #distance_diff_mag,
    z_turn,
    #diff_turn_angle_mag
    distance
  ) |>
  collect() |>
  # group_by(game_id, play_id, player_name, player_name_def) |>
  # summarise(
  #   max_z = max(z_turn, na.rm = TRUE)
  # )
  filter(player_name == 'Mike Evans') |> #summarise(max(z_turn), .by = player_name_def)
  pivot_longer(-c(frame_id, player_name, player_name_def)) |>
  ggplot(aes(frame_id, value, color = player_name_def)) +
  geom_line() +
  facet_wrap(~name, scales = 'free_y', ncol = 1) +
  theme(legend.position = 'top')


play <- tibble(game_id = 2023100808, play_id = 3368)


tbl(con, "combined_data") |>
  play_filter(play[1, c('game_id', 'play_id')]) |>
  summarise(n = n_distinct(frame_id), min_frame = 8) |>
  mutate(x = 52 - 8)
summarise(min(frame_id), .by = player_name)

tbl(con, "combined_data") |>
  play_filter(play) |>
  #play_filter(play |> filter(player_name == 'Trent Sherfield') |> select(-player_name)) |>
  #filter(game_id == 2023090700, play_id == 101) |>
  collect() |>
  ggplot(aes(x, y)) +
  geom_point(aes(alpha = frame_id, color = turn_angle)) +
  geom_text(aes(label = player_name), size = 3, data = \(x) {
    x |> filter(frame_id == min(frame_id))
  })


tbl(con, "pair_features") |>
  filter(player_name_def == 'Tyrique Stevenson') |>
  play_filter(play) |>
  group_by(player_name) |>
  summarise(mean(pred_coverage))


# --
get_model_df() |>
  #play_filter(play) |>
  collect() |>
  #inner_join(play) |>
  #filter(game_id == 2023090700, play_id == 101) |>
  group_by(game_id, play_id, player_name, player_name_def) %>%
  arrange(frame_id, .by_group = TRUE) %>%
  mutate(
    turn_angle_mag = abs(diff_turn_angle),
    angle_mag = abs(angle_with_def_diff),
    # id
    sep = abs(z_turn) > 2.5,
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
    break_frame = first(frame_id),
    break_distance = first(distance),
    break_z_turn = first(z_turn),
    break_angle = first(angle_with_def),
    min_distance = min(distance),
    max_distance = max(distance),
    max_separation_gained = max_distance - break_distance,
    peak_separation_frame = frame_id[which.max(distance)],

    t_max_sep = max(greatest_distance_increasing, na.rm = TRUE),
    #last_increasing_frame = max(frame_id[distance_increasing], na.rm = TRUE),

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
  filter(cov_mean > 0.5, cov_peak > 0.5) |>
  select(-starts_with('cov')) |>
  mutate(
    frames_created = t_max_sep - break_frame
  ) |>
  arrange(-max_sep) -> df

df |>
  ggplot(aes(max_sep)) +
  geom_histogram()


df %>%
  inner_join(play) |>
  group_by(game_id, play_id, player_name, player_name_def) %>%
  arrange(frame_id, .by_group = TRUE) %>%
  mutate(
    # Identify separation breaks
    sep_signal = z_turn > 3,

    # Create separation episodes
    sep_id = cumsum(sep_signal),

    # Track distance changes within each episode
    in_separation = sep_id > 0,
    distance_increasing = distance_diff > 0,
    distance_decreasing = distance_diff < 0
  ) %>%
  filter(sep_id > 0) %>%
  group_by(game_id, play_id, player_name, player_name_def, sep_id) %>%
  summarise(
    # 1. Break point identification
    break_frame = first(frame_id),
    break_distance = first(distance),
    break_z_turn = first(z_turn),
    break_angle = first(angle_with_def),

    # 2. Separation duration & quality
    sep_duration = n(), # frames of separation
    max_distance = max(distance),
    max_separation_gained = max_distance - break_distance,
    peak_separation_frame = frame_id[which.max(distance)],
    frames_to_peak = peak_separation_frame - break_frame,

    # 3. Defender recovery analysis
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

    # Edge case flags
    separation_maintained = !defender_closed_gap &
      last(distance) >= max_distance * 0.9,
    defender_gave_up = !defender_closed_gap &
      mean(tail(pred_coverage, 3), na.rm = TRUE) < 0.3,

    # End state
    final_distance = last(distance),
    final_separation = final_distance - break_distance,

    # Additional context
    avg_coverage_during_sep = mean(pred_coverage, na.rm = TRUE),
    coverage_at_peak = pred_coverage[which.max(distance)],
    max_turn_angle = max(abs(diff_turn_angle), na.rm = TRUE),
    cov_mean = mean(pred_coverage),
    cov_turn = first(pred_coverage)
  ) %>%
  ungroup() |>
  glimpse()
