library(tidyverse)
library(duckdb)

con <- dbConnect(duckdb::duckdb())


get_supplementary_data <- function() {
  readr::read_csv(here::here('data', 'supplementary_data.csv')) |>
    mutate(
      distance_to_goal = if_else(
        possession_team == yardline_side,
        100 - yardline_number,
        yardline_number
      )
    )
}
get_supplementary_data()


get_player_data <- function() {
  dbGetQuery(
    con,
    "
    select distinct nfl_id
    , player_height
    , player_weight
    from read_csv('data/train/input*.csv', quote = '\"')
  "
  ) |>
    as_tibble() |>
    mutate(
      player_height = stringr::str_split(player_height, '-'),
      player_height = map(player_height, as.numeric),
      player_height = map_dbl(.x = player_height, ~ .x[1] * 12 + .x[2]),
      player_height_z = (player_height - mean(player_height)) /
        sd(player_height),
      player_weight_z = (player_weight - mean(player_weight)) /
        sd(player_weight)
    )
}

get_player_data()

load_raw_data <- function(con) {
  cli::cli_alert_info("Loading raw data to duckdb")
  cli::cli_alert_info("Creating supplementary_data")
  dbExecute(
    con,
    "
    drop table if exists supplementary_data;
    create table supplementary_data as
    select *,
      case when possession_team = yardline_side then
        100 - yardline_number else
        yardline_number end as distance_to_goal
      from 'data/supplementary_data.csv'
    "
  )
  cli::cli_alert_info("creating tracking_input")
  dbExecute(
    con,
    glue::glue(
      "
      drop table if exists tracking_input;
      create table tracking_input as
      select t.*, s.week from read_csv('data/train/input_*.csv', quote='\"') t
      left join (select distinct game_id, week from supplementary_data) s on t.game_id = s.game_id
      "
    )
  )
  cli::cli_alert_info("creating tracking_output")
  dbExecute(
    con,
    glue::glue(
      "
        drop table if exists tracking_output;
        create table tracking_output as
        select t.*, s.week from read_csv('data/train/output_*.csv', quote='\"') t
        left join (select distinct game_id, week from supplementary_data) s on t.game_id = s.game_id
        "
    )
  )
  cli::cli_alert_info("creating players")
  dbWriteTable(con, "players", get_player_data(), overwrite = TRUE)
}
load_raw_data(con)

convert_tracking_cortesian <- function(.data) {
  .data |>
    mutate(
      dir = ((dir - 90) * -1) %% 360,
      o = ((o - 90) * -1) %% 360,
      vx = s * cos(dir * pi / 180), # Convert dir to radians for cos
      vy = s * sin(dir * pi / 180), # Convert dir to radians for sin
      ox = cos(o * pi / 180), # Convert o to radians for cos
      oy = sin(o * pi / 180) # Convert o to radians for sin
    )
}


standardize_tracking_directions <- function(.data) {
  .data |>
    mutate(
      x = ifelse(play_direction == 'right', x, 120 - x),
      y = ifelse(play_direction == 'right', y, 53.3 - y),
      vx = ifelse(play_direction == 'right', vx, -1 * vx),
      vy = ifelse(play_direction == 'right', vy, -1 * vy),
      ox = ifelse(play_direction == 'right', ox, -1 * ox),
      oy = ifelse(play_direction == 'right', oy, -1 * oy)
    )
}

augment_mirror_tracking <- function(.data) {
  .data2 <- .data |>
    mutate(
      y = 53.3 - y, # Flip y values
      vy = -1 * vy, # Reverse vy
      oy = -1 * oy, # Reverse oy
      mirrored = TRUE # Mark as mirrored
    )

  .data <- .data |> mutate(mirrored = FALSE)

  .data |>
    union_all(.data2)
}

add_relative_positions <- function(.data) {
  .data |>
    mutate(
      x_rel = x - (100 - distance_to_goal),
      ball_land_x_rel = x - (100 - distance_to_goal)
    )
}

get_prepped_data <- function(con, weeks = NULL) {
  x <- tbl(con, "tracking_input") |>
    left_join(
      tbl(con, "supplementary_data") |>
        select(game_id, play_id, distance_to_goal)
    )

  if (all(is.numeric(weeks))) {
    cli::cli_inform(glue::glue("Filtering for week: {weeks}"))
    x <- x |> filter(week %in% weeks)
  }

  x |>
    convert_tracking_cortesian() |>
    standardize_tracking_directions() |>
    augment_mirror_tracking() |>
    add_relative_positions()
}

get_prepped_data(con) |> count()
get_prepped_data(con, weeks = 1) |> count()

# TODO: create transformations for output
