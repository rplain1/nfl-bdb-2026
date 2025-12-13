df <- tbl(con, "coverage_predictions") |>
  left_join(get_play_starts()) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  dbplyr::window_frame(-2) |>
  mutate(
    pred_coverage = mean(.pred_1)
  ) |>
  filter(frame_id %% 2 == 0) |>
  ungroup() |>
  # group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  # dbplyr::window_order(frame_id) |>
  dbplyr::window_frame() |>
  mutate(
    diff_turn_angle = atan2(
      sin(turn_angle - turn_angle_def),
      cos(turn_angle - turn_angle_def)
    ),
    z_turn = (diff_turn_angle - mean(diff_turn_angle)) / sd(diff_turn_angle)
  ) |>
  ungroup() |>
  mutate(z_turn_pred = z_turn * pred_coverage)


df |>
  play_filter(play |> bind_rows(get_common_plays() |> select(-player_name))) |>
  collect() |>
  mutate(
    z_turn_pred = z_turn * pred_coverage
  ) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  arrange(frame_id, .by_group = TRUE) |>
  mutate(
    z_turn2 = (diff_turn_angle - mean(diff_turn_angle, na.rm = TRUE)) /
      sd(diff_turn_angle, na.rm = TRUE),
  ) |>
  ungroup() |>
  mutate(
    z_turn2 = z_turn2 * pred_coverage
  ) |>
  select(
    game_id,
    frame_id,
    player_name,
    player_name_def,
    .pred_1,
    pred_coverage,
    z_turn_pop = z_turn_pred,
    z_turn_play = z_turn2
  ) |>
  pivot_longer(.pred_1:z_turn_play) |>
  filter(str_detect(name, 'z_turn')) |>
  filter(frame_id > 10) |>
  ggplot(aes(frame_id, value, color = name)) +
  geom_line() +
  geom_hline(yintercept = c(-1, 1), linetype = 'dashed') +
  facet_wrap(
    ~ paste(player_name, player_name_def, sep = '\n'),
    scales = 'free'
  ) +
  scale_color_manual(values = c('darkgrey', '#a42341ff'))


df |>
  group_by(game_id, play_id, player_name, player_name_def) %>%
  dbplyr::window_order(frame_id) %>%
  filter(!(frame_id == min(frame_id) & z_turn_pred == max(z_turn_pred))) |>
  mutate(
    sep = abs(z_turn_pred) > 2,
    sep_id = cumsum(sep),
    distance_diff = distance - lag(distance),
    greatest_distance_increasing = max(
      ifelse(distance_diff > 0, frame_id, 0),
      na.rm = TRUE
    )
  ) |>
  ungroup() |>
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
  ) %>% # Removed sep_id
  summarise(
    ball_in_air = min(ball_in_air, na.rm = TRUE),
    #num_separation_events = max(sep_id, na.rm = TRUE), # Add this to track how many events
    break_frame = first(frame_id),
    break_distance = first(distance),
    break_z_turn = first(z_turn_pred),
    min_distance = min(distance, na.rm = TRUE),
    max_distance = max(distance, na.rm = TRUE),
    max_separation_gained = max_distance - break_distance,
    peak_separation_frame = frame_id[which.max(distance)],
    t_max_sep = max(greatest_distance_increasing, na.rm = TRUE),
    defender_closed_gap = any(
      distance < break_distance & frame_id > peak_separation_frame
    ),
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
    max_sep = max(ifelse(sep_id > 0, distance, 0), na.rm = TRUE), # Fixed this logic
    cov_mean = mean(pred_coverage),
    cov_peak = pred_coverage[which.max(distance_diff)],
    .groups = 'drop'
  ) |>
  filter(cov_peak > 0.5) |>
  mutate(
    frames_created = t_max_sep - break_frame
  ) |>
  arrange(-max_sep) -> df_output


one_defender = df_output |>
  count(game_id, play_id, nfl_id, sep_id, sort = T) |>
  filter(n == 1)


df_output |>
  inner_join(one_defender) |>
  group_by(nfl_id, player_name) |>
  summarise(x = mean(max_sep), n = n()) |>
  arrange(-n)

df_output |> ggplot(aes(max_sep)) + geom_histogram()
df_output |> ggplot(aes(frames_created / 10)) + geom_histogram()


df_output |>
  group_by(nfl_id, player_name, player_position, game_id, play_id) |>
  summarise(
    win = max(sep_id > 0),
    .groups = 'drop'
  ) |>
  group_by(nfl_id, player_name, player_position) |>
  summarise(
    win = sum(win),
    n = n(),
    .groups = 'drop'
  ) |>
  ggplot(aes(n, max_sep)) +
  geom_smooth(aes(color = player_position), method = 'lm') +
  geom_point() +
  ggrepel::geom_text_repel(aes(label = player_name))


hop <- df_output |>
  filter(player_name == 'DeAndre Hopkins') |>
  arrange(-max_sep) |>
  select(game_id, play_id)

play = hop[1:2, ]

plot_play(play)

df |>
  play_filter(play) |>
  ggplot(aes(frame_id, pred_coverage)) +
  geom_line() +
  facet_wrap(~ paste(player_name, player_name_def, sep = '\n'))

df |>
  play_filter(play) |>
  ggplot(aes(frame_id, z_turn * pred_coverage)) +
  geom_line() +
  facet_wrap(~ paste(player_name, player_name_def, sep = '\n'))


tbl(con, "supplementary_data") |>
  play_filter(play) |>
  select(team_coverage_type)


play = hop[1:2, ]

df |>
  play_filter(play) |>
  ggplot(aes(frame_id, .pred_1)) +
  geom_line() +
  facet_wrap(~ paste(player_name, player_name_def, sep = '\n'))
