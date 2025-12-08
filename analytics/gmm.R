library(tidyverse)
library(mclust)
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
