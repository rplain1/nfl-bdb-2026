library(tidyverse)
#con <- duckdb::dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

tbl(con, "distances") |>
  filter(
    player_position %in% c('WR', 'TE', 'RB'),
    player_to_predict,
    player_to_predict_def
  ) |>
  # group_by(game_id, play_id, off_id, def_id) |>
  # mutate(min_dist = min(distance)) |>
  # ungroup() |>
  # filter(min_dist <= 10) |>
  mutate(y = y - (53.3 / 2), y_def = y_def - (53.3 / 2)) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    dist_change = abs(distance - lag(distance)),
    min_dist_last_5 = sql(
      "
      MIN(distance) OVER (
        PARTITION BY game_id, play_id, nfl_id, nfl_id_def
        ORDER BY frame_id
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
      )
    "
    ),
    delta_x = x_def - x,
    delta_y = y_def - y,
    rel_speed_toward = (x - x_def) * vx_def + (y - y_def) * vy_def,
    mag_receiver = sqrt(vx^2 + vy^2),
    mag_defender = sqrt(vx_def^2 + vy_def^2),
    dot_prod = vx * vx_def + vy * vy_def,
    velocity_alignment = dot_prod / (mag_receiver * mag_defender),
    player_position_def = case_when(
      player_position_def %in% c('ILB', 'MLB', 'OLB', 'LB') ~ 'LB',
      player_position_def %in% c('FS', 'S', 'SS') ~ 'S',
      player_position_def %in% c('DT', 'NT', 'DE') ~ 'DT',
      TRUE ~ player_position_def
    )
  ) |>
  ungroup() |>
  filter(frame_id > frame_start) |>
  select(
    game_id,
    play_id,
    nfl_id,
    nfl_id_def,
    player_name,
    player_name_def,
    frame_id,
    distance,
    dist_change,
    min_dist_last_5,
    velocity_alignment,
    rel_speed_toward,
    delta_x,
    delta_y,
    player_position,
    player_position_def
  ) |>
  left_join(
    tbl(con, "supplementary_data") |>
      select(game_id, play_id, team_coverage_man_zone)
  ) |>
  collect() |>
  filter(
    !is.na(team_coverage_man_zone),
    team_coverage_man_zone != 'NA'
  ) -> df_features


colSums(is.na(df_features))
df_features <- na.omit(df_features)


library(recipes)


X <- recipes::recipe(~., data = df_features) |>
  recipes::update_role(
    game_id,
    play_id,
    nfl_id,
    nfl_id_def,
    frame_id,
    player_name,
    player_name_def,
    new_role = "ID"
  ) |>
  recipes::step_dummy(
    team_coverage_man_zone,
    player_position,
    player_position_def
  ) |>
  recipes::step_scale(recipes::all_predictors()) |>
  recipes::prep() |>
  recipes::bake(new_data = df_features)

X <- X |> select(where(is.numeric), -contains('_id'))
#BIC <- mclustBIC(X)

library(mclust)
set.seed(52723)
mod <- Mclust(X, G = 2)

probs <- as.data.frame(mod$z)

df_with_preds <- df_features |> bind_cols(probs)


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


tbl(con, "combined_data") |>
  play_filter(play) |>
  collect() |>
  ggplot(aes(x, y)) +
  geom_point(aes(alpha = frame_id, color = dataset)) +
  geom_text(aes(label = player_name), size = 3, data = \(x) {
    x |> filter(frame_id == min(frame_id))
  })

df_with_preds |>
  inner_join(play) |>
  group_by(game_id, play_id, nfl_id_def) |>
  arrange(frame_id, .by_group = TRUE) |>
  mutate(pred_coverage = cummean(V2)) |>
  filter(player_name == 'Mike Evans') |>
  ggplot(aes(frame_id)) +
  geom_line(aes(y = V2), color = 'blue') +
  geom_line(aes(y = pred_coverage), color = 'red') +
  facet_wrap(~player_name_def)


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
  filter(player_name == 'Mike Evans') |>
  pivot_longer(-c(frame_id, player_name, player_name_def)) |>
  ggplot(aes(frame_id, value, color = player_name_def)) +
  geom_line() +
  facet_wrap(~name, scales = 'free_y', ncol = 1)
