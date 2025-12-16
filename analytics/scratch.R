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
    peak_recovery_gap_frame = ifelse()
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

# sun god 2 event play
# sup_data |> play_filter(tibble(game_id = 2023090700, play_id = 2613))

#' Integrated Separation Index (ISI) is the sum of a receiver’s maximum spatial
#' separation and the cumulative distance by which the defender failed to recover,
#' producing a distance-equivalent measure of route impact.

dt = 1 / 10
df |>
  filter(cov_max > 0.5) |>
  group_by(game_id, play_id, player_name, player_name_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    z_turn_pop = ifelse(
      frame_id == min(frame_id) & z_turn_pop == max(z_turn_pop),
      0,
      z_turn_pop
    ),

    # Identify break: first sustained elevation
    sep_trigger = abs(z_turn_pop) > 2.5 &
      abs(z_turn_play) > 2 &
      lag(z_turn_play) < 2,
    sep_id = cumsum(sep_trigger),

    distance_diff = distance - lag(distance)
  ) |>
  filter(sep_id > 0) |>
  collect() |>
  # Now group by break event
  group_by(
    game_id,
    play_id,
    player_name,
    player_name_def,
    player_position,
    sep_id
  ) |>
  mutate(
    # Context at break
    d_at_break = first(distance),
    frame_at_break = first(frame_id),

    # Reverse cummin: best they'll achieve from here forward
    d_achievable = rev(cummin(rev(distance))),

    # Separation excess
    sep_excess = pmax(distance - d_achievable, 0),

    # AUSC contribution
    ausc_contrib = sep_excess * dt
  ) |>
  summarize(
    frame_start = first(frame_id),
    ausc = sum(ausc_contrib, na.rm = TRUE),
    d_at_break = first(d_at_break),
    break_zcore = first(z_turn_pop),
    max_distance = max(distance, na.rm = TRUE),
    peak_separation = max(sep_excess, na.rm = TRUE),
    frames_separated = sum(sep_excess > 0, na.rm = TRUE),
    weighted_unrecovered = sum(peak_separation * frames_separated * dt) /
      sum(frames_separated * dt),
    .groups = "drop"
  ) |>
  mutate(ausc_score = ausc / (max_distance * frames_separated * dt)) -> .df


.df |>
  inner_join(get_common_plays()) |>
  mutate(ausc_score = ausc / (max_distance * frames_separated * dt))


.df |>
  ggplot(aes(frames_separated, peak_separation)) +
  geom_point() +
  ggrepel::geom_text_repel(aes(label = player_name))

.df |>
  ggplot(aes(SEP_DELTA)) +
  geom_histogram()


lm(max_distance ~ ausc_score + frames_separated, data = .df) |> summary()
