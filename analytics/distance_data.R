library(tidyverse)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")


#' this is a dataset that gets the start of a play, where the players on offense
#' besides the QB have all had a speed of 1 or greater
get_play_starts <- function() {
  tbl(con, "combined_data") |>
    filter(player_side == 'Offense', player_position != 'QB') |>
    mutate(s_filter = s > 1) |>
    filter(s_filter) |>
    group_by(game_id, play_id, nfl_id) |>
    summarise(min_frame = min(frame_id), .groups = 'drop') |>
    group_by(game_id, play_id) |>
    summarise(frame_start = max(min_frame), .groups = 'drop') |>
    compute(name = 'play_starts', overwrite = TRUE)
}


#' ------- Start Distance datasets -----------------

select_distance_cols <- function(.data) {
  .data |>
    select(
      game_id,
      play_id,
      nfl_id,
      week,
      frame_id,
      player_name,
      player_to_predict,
      player_position,
      player_side,
      player_role,
      dataset,
      x,
      y,
      o,
      vx,
      vy,
      a,
      o,
      s,
      s_mph,
      turn_angle
    )
}


create_distance_data <- function(con) {
  get_play_starts()

  offensive <- tbl(con, "combined_data") |>
    filter(
      player_side == 'Offense',
      # dataset == 'X',
      player_position != 'QB'
    ) |>
    select_distance_cols()

  defensive <- tbl(con, "combined_data") |>
    filter(player_side == 'Defense') |>
    select_distance_cols()

  distances <- offensive |>
    inner_join(
      defensive,
      by = c('game_id', 'play_id', 'frame_id'),
      suffix = c('', '_def')
    ) |>
    mutate(
      distance = sqrt((x - x_def)^2 + (y - y_def)^2),
    )
}

load_distance_data <- function(con) {
  create_distance_data(con) |>
    left_join(tbl(con, "play_starts"), by = c('game_id', 'play_id')) |>
    compute(name = 'distances', overwrite = TRUE, temporary = FALSE)
}
