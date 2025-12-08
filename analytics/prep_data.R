library(tidyverse)
library(duckdb)

con <- dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

#' Read in the supplementary data and correct the distance_to_goal
#' field
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

#' player data, aggregated at the player level across all the
#' input tracking data.
get_player_data <- function() {
  dbGetQuery(
    con,
    "
    select distinct nfl_id
    , player_name
    -- , player_height
    , player_weight
    from read_csv('data/train/input*.csv', quote = '\"')
  "
  ) |>
    as_tibble() |>
    mutate(
      #   player_height = stringr::str_split(player_height, '-'),
      #   player_height = map(player_height, as.numeric),
      #   player_height = map_dbl(.x = player_height, ~ .x[1] * 12 + .x[2]),
      #   player_height_z = (player_height - mean(player_height)) /
      #     sd(player_height),
      player_weight_z = (player_weight - mean(player_weight)) /
        sd(player_weight)
    )
}

#' Should probably improve this from SQL, but loads everything in to
#' a duckdb connection.
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
        select t.*, s.week, s.distance_to_goal, ti.play_direction
        from read_csv('data/train/output_*.csv', quote='\"') t
        left join (select distinct game_id, play_id, week, distance_to_goal from supplementary_data) s on t.game_id = s.game_id and t.play_id = s.play_id
        left join (select distinct game_id, play_id, play_direction from tracking_input) ti on ti.game_id = t.game_id and ti.play_id = t.play_id
        "
    )
  )
  cli::cli_alert_info("creating players")
  dbWriteTable(con, "players", get_player_data(), overwrite = TRUE)
}


# convert_tracking_cortesian <- function(.data) {
#   .data |>
#     mutate(
#       dir = ((dir - 90) * -1) %% 360,
#       o = ((o - 90) * -1) %% 360,
#       vx = s * cos(dir * pi / 180), # Convert dir to radians for cos
#       vy = s * sin(dir * pi / 180), # Convert dir to radians for sin
#       ox = cos(o * pi / 180), # Convert o to radians for cos
#       oy = sin(o * pi / 180) # Convert o to radians for sin
#     )
# }

#' Basic adjustments to align all of the coordinates so that
#' LOS is at 0 for x_rel.
#' `TODO``: look into having y having half positive and negative
standardize_tracking_directions <- function(.data) {
  .data <- .data |>
    mutate(
      x = ifelse(play_direction == 'right', x, 120 - x),
      y = ifelse(play_direction == 'right', y, 53.3 - y),
      x = x - (110 - distance_to_goal),
    )

  if (all('ball_land_x' %in% colnames(.data))) {
    .data <- .data |>
      mutate(
        ball_land_x = ifelse(
          play_direction == 'right',
          ball_land_x,
          110 - ball_land_x
        ),
        ball_land_y = ifelse(
          play_direction == 'right',
          ball_land_y,
          53.3 - ball_land_y
        ),
        ball_land_x_rel = ball_land_x - (110 - distance_to_goal)
        #   vx = ifelse(play_direction == 'right', vx, -1 * vx),
        #   vy = ifelse(play_direction == 'right', vy, -1 * vy),
        #   ox = ifelse(play_direction == 'right', ox, -1 * ox),
        #   oy = ifelse(play_direction == 'right', oy, -1 * oy),
      )
  }

  .data
}

join_tracking_data <- function(con, weeks = NULL) {
  x <- tbl(con, "tracking_input") |>
    left_join(
      tbl(con, "supplementary_data") |>
        select(game_id, play_id, distance_to_goal)
    )

  if (all(is.numeric(weeks))) {
    cli::cli_inform(glue::glue("Filtering for week: {weeks}"))
    x <- x |> filter(week %in% weeks)
  }

  x <- x |>
    mutate(dataset = 'X') |>
    standardize_tracking_directions()

  y <- tbl(con, "tracking_output")
  if (all(is.numeric(weeks))) {
    cli::cli_inform(glue::glue("Filtering for week: {weeks}"))
    y <- y |> filter(week %in% weeks)
  }

  y <- y |>
    mutate(dataset = 'Y') |>
    standardize_tracking_directions()

  x |>
    select(
      -s,
      -a,
      -dir,
      -num_frames_output,
      -player_height,
      -absolute_yardline_number,
      -player_name,
      -player_position,
      -player_side,
      -ball_land_x,
      -ball_land_y,
      -player_to_predict
    ) |>
    union_all(y, by = 'name') |>
    group_by(game_id, play_id, nfl_id) |>
    dbplyr::window_order(dataset, frame_id) |>
    mutate(frame_id = row_number()) |>
    ungroup() |>
    inner_join(
      x |> distinct(game_id, play_id, ball_land_x, ball_land_y)
    ) |>
    inner_join(
      x |>
        distinct(
          game_id,
          play_id,
          nfl_id,
          player_name,
          player_side,
          player_position,
          player_to_predict
        )
    )
}


#' Tracking features calculated, requires grouping by game_id,
#' play_id, nfl_id and then arrange by frame_id.
add_tracking_features <- function(.data) {
  .data |>
    group_by(game_id, play_id, nfl_id) |>
    dbplyr::window_order(frame_id) |>
    mutate(
      dx = lead(x) - x,
      dy = lead(y) - y,
      dt = 1 / 10,
      vx = dx / dt,
      vy = dy / dt,
      s = sqrt(vx^2 + vy^2),
      s_mph = s * (3600 / 1760),
      ax = (lead(vx) - vx) / dt,
      ay = (lead(vy) - vy) / dt,
      a = sqrt(ax^2 + ay^2),
      # dir = atan2(vy, vx) * 180 / pi,
      # dir = (dir + 360) %% 360,
      # x_end = s * cos((90 - dir) * pi / 180) + x,
      # y_end = s * sin((90 - dir) * pi / 180) + y,
      turn_angle = atan2(
        lead(y) - y,
        lead(x) - x
      ) -
        atan2(
          y - dplyr::lag(y),
          x - dplyr::lag(x)
        ),
      turn_angle = ifelse(
        turn_angle >= pi,
        turn_angle - 2 * pi,
        ifelse(turn_angle <= -pi, 2 * pi + turn_angle, turn_angle)
      ),
      turn_angle = ifelse(
        turn_angle > 3.14 | turn_angle < -3.14,
        0,
        turn_angle
      ),
      prev_angle = dplyr::lag(turn_angle)
    ) |>
    ungroup()
}


prep_data <- function(con, weeks = NULL, load_data = FALSE, return_df = FALSE) {
  if (load_data) {
    load_raw_data(con)
  }

  cli::cli_alert_info("creating combined_data")
  clean_df <- join_tracking_data(con, weeks) |>
    add_tracking_features()

  if (return_df) {
    clean_df |> as_tibble()
  } else {
    clean_df |>
      compute(name = 'combined_data', overwrite = TRUE, temporary = FALSE)
    cli::cli_alert_success(
      "Combined tracking data available in `combined_data`!"
    )
  }
}
