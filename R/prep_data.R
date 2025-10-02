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
        select t.*, s.week, s.distance_to_goal, ti.play_direction
        from read_csv('data/train/output_*.csv', quote='\"') t
        left join (select distinct game_id, play_id, week, distance_to_goal from supplementary_data) s on t.game_id = s.game_id and t.play_id = s.play_id
        left join (select distinct game_id, play_id, play_direction from tracking_input) ti on t.game_id = t.game_id and ti.play_id = t.play_id
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
  .data <- .data |>
    mutate(
      x = ifelse(play_direction == 'right', x, 120 - x),
      y = ifelse(play_direction == 'right', y, 53.3 - y),
    )

  if (all('vx' %in% colnames(.data))) {
    cli::cli_alert_info("`vy` detected, applying additional transformations")
    .data <- .data |>
      mutate(
        vx = ifelse(play_direction == 'right', vx, -1 * vx),
        vy = ifelse(play_direction == 'right', vy, -1 * vy),
        ox = ifelse(play_direction == 'right', ox, -1 * ox),
        oy = ifelse(play_direction == 'right', oy, -1 * oy)
      )
  }

  .data
}

augment_mirror_tracking <- function(.data) {
  .data2 <- .data |>
    mutate(
      y = 53.3 - y, # Flip y values
      mirrored = TRUE # Mark as mirrored
    )

  if (all('vy' %in% colnames(.data))) {
    cli::cli_alert_info("`vy` detected, applying additional transformations")
    .data2 <- .data2 |>
      mutate(
        vy = -1 * vy, # Reverse vy
        oy = -1 * oy, # Reverse oy
      )
  }

  .data <- .data |> mutate(mirrored = FALSE)

  .data |>
    union_all(.data2)
}

add_relative_positions <- function(.data) {
  .data <- .data |>
    mutate(
      x_rel = x - (100 - distance_to_goal),
    )

  if (all("ball_land_x") %in% names(.data)) {
    .data <- .data |>
      mutate(
        ball_land_x_rel = ball_land_x_rel - (100 - distance_to_goal)
      )
  }
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

get_prepped_output <- function(con, weeks = NULL) {
  x <- tbl(con, "tracking_output")
  if (all(is.numeric(weeks))) {
    cli::cli_inform(glue::glue("Filtering for week: {weeks}"))
    x <- x |> filter(week %in% weeks)
  }

  x |>
    standardize_tracking_directions() |>
    augment_mirror_tracking() |>
    add_relative_positions()
}

get_ids_list <- function(df) {
  plays <- df |>
    distinct(game_id, play_id, mirrored) |>
    collect()

  train_ids <- plays |>
    slice_sample(prop = 0.7)

  non_train_ids <- plays |>
    anti_join(train_ids)

  val_ids <- non_train_ids |>
    slice_sample(prop = 0.5)

  test_ids <- non_train_ids |>
    inner_join(val_ids)

  list(
    train = train_ids,
    val = val_ids,
    test = test_ids
  )
}

split_data <- function(df, ids) {
  df |>
    inner_join(ids)
}


prep_data <- function(con, week = NULL) {
  x <- get_prepped_data(con, week) |> collect()
  y <- get_prepped_output(con, week) |> collect()
  ids <- get_ids_list(x)

  purrr::walk(names(ids), function(id) {
    cli::cli_alert(id)

    # Process data
    features <- split_data(x, ids[[id]])
    targets <- split_data(y, ids[[id]])

    # Validate data alignment with informative error message
    missing_plays <- nrow(anti_join(
      features,
      targets,
      by = c('game_id', 'play_id', 'mirrored')
    ))

    if (missing_plays != 0L) {
      cli::cli_abort("Found {missing_plays} mismatched play(s) in {id}")
    }

    # Use file.path for cross-platform compatibility
    output_dir <- "prepped_data"
    file_name_features <- file.path(output_dir, paste0(id, "_features.parquet"))
    file_name_targets <- file.path(output_dir, paste0(id, "_targets.parquet"))

    # Write files with progress info
    cli::cli_alert_info("Writing {nrow(features)} feature rows")
    arrow::write_parquet(features, file_name_features)

    cli::cli_alert_info("Writing {nrow(targets)} target rows")
    arrow::write_parquet(targets, file_name_targets)
  })
}
