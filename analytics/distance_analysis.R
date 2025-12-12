library(tidyverse)
library(duckdb)
con <- duckdb::dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

play <- tibble(
  game_id = c(
    2023091706,
    2023091710,
    2023101506,
    2023092406,
    2023091706,
    2023090700
  ),
  play_id = c(2583, 3659, 3367, 2741, 3512, 361),
  player_name = c(
    'Mike Evans',
    'Garrett Wilson',
    'Josh Downs',
    'Mike Williams',
    "Chase Claypool",
    "Marvin Jones"
  )
)

jamar_chase_play <- get_supplementary_data() |>
  filter(
    week == 1,
    possession_team == 'CIN',
    str_detect(play_description, "14:11")
  ) |>
  select(play_id, game_id)

tbl(con, "combined_data") |>
  play_filter(play) |>
  collect() |>
  ggplot(aes(x, y)) +
  geom_point(aes(alpha = frame_id, color = dataset)) +
  geom_text(aes(label = player_name), size = 3, data = \(x) {
    x |> filter(frame_id == min(frame_id))
  })


dbWriteTable(con, "coverage_assignments", df_with_preds, overwrite = TRUE)

coverage_assignments <- tbl(con, "coverage_assignments")
combined_data <- tbl(con, "combined_data") |>
  select(
    game_id,
    play_id,
    frame_id,
    nfl_id,
    x,
    y,
    vx,
    vy,
    a,
    s,
    s_mph,
    turn_angle
  )

coverage_assignments |>
  select(
    game_id,
    play_id,
    player_name,
    player_name_def,
    distance,
    nfl_id,
    nfl_id_def,
    frame_id,
    pred_coverage = V2
  ) |>
  left_join(
    combined_data,
    by = c('game_id', 'play_id', 'frame_id', 'nfl_id')
  ) |>
  left_join(
    combined_data,
    by = c('game_id', 'play_id', 'frame_id', 'nfl_id_def' = 'nfl_id'),
    suffix = c('', '_def')
  ) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    dx_pair = x_def - x,
    dy_pair = y_def - y,
    angle_with_def = atan2(dy_pair, dx_pair),

    # previous frame’s angle
    angle_with_def_prev = lag(angle_with_def),

    # proper wrapped angular difference
    angle_with_def_diff = atan2(
      sin(angle_with_def - angle_with_def_prev),
      cos(angle_with_def - angle_with_def_prev)
    ),
    angle_with_def_diff_mag = angle_with_def_diff - lag(angle_with_def_diff),
    angle_with_def_diff_sign = sign(angle_with_def_diff_mag),
    diff_turn_angle = atan2(
      sin(turn_angle - turn_angle_def),
      cos(turn_angle - turn_angle_def)
    )
  ) |>
  dbplyr::window_frame(-5) |>
  mutate(pred_coverage = mean(pred_coverage)) |>
  ungroup() |>
  collect() -> df

df |>
  mutate(
    across(
      c(angle_with_def_diff, angle_with_def_diff_mag, diff_turn_angle),
      ~ .x * pred_coverage
    )
  ) |>
  inner_join(play) |>
  select(
    frame_id,
    player_name,
    player_name_def,
    diff_turn_angle,
    #angle_with_def,
    angle_with_def_diff,
    angle_with_def_diff_mag,
    pred_coverage
  ) |>
  mutate(pred_coverage = round(pred_coverage, 2)) |>
  filter(player_name == 'Mike Evans') |>
  pivot_longer(-c(frame_id, player_name, player_name_def)) |>
  ggplot(aes(frame_id, value, color = player_name_def)) +
  geom_line() +
  facet_wrap(~name, scales = 'free_y', ncol = 1)


tbl(con, "pair_features") |>
  group_by(game_id, nfl_id, play_id) |>
  summarise(diff_turn_angle = max(abs(diff_turn_angle), na.rm = TRUE)) |>
  select(diff_turn_angle) |>
  collect() |>
  ggplot() +
  geom_histogram(aes(diff_turn_angle), bins = 50)


tbl(con, "pair_features") |>
  #group_by(game_id, nfl_id, play_id) |>
  filter(frame_id > min(frame_id) + 2) |>
  mutate(
    z = (diff_turn_angle - mean(diff_turn_angle, na.rm = TRUE)) /
      sd(diff_turn_angle, na.rm = TRUE)
  ) |>
  group_by(game_id, nfl_id, play_id) |>
  summarise(
    diff_turn_angle = max(abs(z), na.rm = TRUE)
  ) |>
  ggplot() +
  geom_histogram(aes(diff_turn_angle))


tbl(con, "pair_features") |>
  left_join(tbl(con, "combined_data") |> distinct(nfl_id, player_position)) |>
  group_by(game_id, nfl_id, play_id) |>
  filter(frame_id > min(frame_id) + 2) |>
  ungroup() |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  mutate(
    diff_turn_angle = abs(diff_turn_angle),
    z_turn_angle_diff = (diff_turn_angle -
      mean(diff_turn_angle, na.rm = TRUE)) /
      sd(diff_turn_angle, na.rm = TRUE),
    z_angle_def_mag = (angle_with_def_diff_mag -
      mean(angle_with_def_diff_mag, na.rm = TRUE)) /
      sd(angle_with_def_diff_mag, na.rm = TRUE)
  ) |>
  inner_join(
    tbl(con, 'combined_data') |>
      filter(dataset == 'Y') |>
      distinct(play_id, game_id, frame_id)
  ) |>
  group_by(
    game_id,
    nfl_id,
    play_id,
    player_name,
    player_name_def,
    player_position
  ) |>
  summarise(
    n = n(),
    z_turn_angle = max(abs(z_turn_angle_diff), na.rm = TRUE),
    z_angle_mag_min = min(z_angle_def_mag, na.rm = TRUE),
    z_angle_mag_max = max(z_angle_def_mag, na.rm = TRUE),
    .groups = 'drop'
  ) |>
  #play_filter(play) |>
  mutate(
    win = z_turn_angle > 2 & z_angle_mag_min < 2 & z_angle_mag_max > 2
  ) |>
  group_by(game_id, play_id, nfl_id, player_name, player_position) |>
  summarise(
    win = max(win, na.rm = TRUE)
  ) |>
  filter(player_name == 'Mike Evans', win == TRUE)
group_by(nfl_id, player_name, player_position) |>
  summarise(
    n = n(),
    win = sum(win),
    .groups = 'drop'
  ) |>
  filter(player_position == 'WR') |>
  mutate(win_perc = win / n) |> #collect() |> count(round(n, -1)) |> mutate(nn = cumsum(n)/sum(n))
  arrange(-n) |>
  collect() |>
  ggplot(aes(n, win)) +
  geom_point() +
  geom_smooth(method = 'lm') +
  ggrepel::geom_text_repel(aes(label = player_name), max.overlaps = 5)
