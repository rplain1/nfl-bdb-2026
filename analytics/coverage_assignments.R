library(tidyverse)
library(duckdb)
#con <- duckdb::dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

create_gmm_model_data <- function(con) {
  tbl(con, "distances") |>
    filter(
      player_position %in% c('WR', 'TE', 'RB'),
      player_to_predict,
      player_to_predict_def
    ) |>
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
    ) |>
    na.omit()
}

create_gmm_model_matrix <- function(df) {
  X <- recipes::recipe(~., data = df) |>
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
    recipes::bake(new_data = df)

  X <- X |> select(where(is.numeric), -contains('_id'))
}

fit_gmm <- function(X) {
  set.seed(52723)
  mod <- mclust::Mclust(X, G = 2)
  as.data.frame(mod$z)
}

run_gmm_model <- function(con) {
  cli::cli_alert_info("Prepping data for GMM")
  library(mclust)

  df_features <- create_gmm_model_data(con)
  X <- create_gmm_model_matrix(df_features)
  cli::cli_alert_info("Fitting GMM")
  probs <- fit_gmm(X)

  if (mean(probs[, 1]) > mean(probs[, 2])) {
    probs <- probs[, c(2, 1)] # Swap columns
  }

  dbWriteTable(
    con,
    "coverage_assignments",
    df_features |> bind_cols(probs),
    overwrite = TRUE
  )
  cli::cli_alert_success(
    "GMM predicitons written to table `coverage_assignments`"
  )
}


create_pair_features <- function(con) {
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
      angle_with_def_prev = lag(angle_with_def),
      angle_with_def_diff = atan2(
        sin(angle_with_def - angle_with_def_prev),
        cos(angle_with_def - angle_with_def_prev)
      ),
      angle_with_def_diff_mag = angle_with_def_diff - lag(angle_with_def_diff),
      diff_turn_angle = atan2(
        sin(turn_angle - turn_angle_def),
        cos(turn_angle - turn_angle_def)
      )
    ) |>
    dbplyr::window_frame(-5) |>
    mutate(pred_coverage = mean(pred_coverage)) |>
    ungroup() |>
    collect() -> df

  df <- df |>
    mutate(
      across(
        c(angle_with_def_diff, angle_with_def_diff_mag, diff_turn_angle),
        ~ .x * pred_coverage
      )
    )

  dbWriteTable(con, "pair_features", df, overwrite = TRUE)
  cli::cli_alert_success(
    "Receiver/defender features available in `pair_features`!"
  )
}


load_coverage_data <- function(con, run_gmm = FALSE) {
  if (run_gmm) {
    cli::cli_alert_info('Running GMM with `run_gmm = TRUE`')
    run_gmm_model(con)
  }
  create_pair_features(con)
}
