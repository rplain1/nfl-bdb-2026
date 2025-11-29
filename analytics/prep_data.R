library(tidyverse)
library(duckdb)

con <- dbConnect(duckdb::duckdb())

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
load_raw_data(con)

#' distance helper function
get_distance <- function(.data, player_names) {
  .data |>
    filter(player_name %in% player_names) |>
    summarize(
      distance = sqrt(diff(x)^2 + diff(y)^2),
      .by = frame_id2
    )
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
      x_rel = x - (110 - distance_to_goal),
    )

  if (all('ball_land_x' %in% colnames(.data))) {
    .data <- .data |>
      mutate(
        ball_land_x = ifelse(
          play_direction == 'right',
          ball_land_x,
          120 - ball_land_x
        ),
        ball_land_y = ifelse(
          play_direction == 'right',
          ball_land_y,
          53.3 - ball_land_y
        ),
        ball_land_x_rel = ball_land_x - (110 - distance_to_goal),
      )
  }

  .data
}

#' Tracking features calculated, requires grouping by game_id,
#' play_id, nfl_id and then arrange by frame_id2.
#' `TODO``: this is currently ran in R but could also be run in duckdb
#' using `dbplyr::window_order()`
add_tracking_features <- function(.data) {
  .data |>
    mutate(
      dx = lead(x_rel) - x_rel,
      dy = lead(y) - y,
      dt = 1 / 10,
      vx = dx / dt,
      vy = dy / dt,
      s = sqrt(vx^2 + vy^2),
      s_mph = s * (3600 / 1760),
      ax = (lead(vx) - vx) / dt,
      ay = (lead(vy) - vy) / dt,
      accel = sqrt(ax^2 + ay^2),
      dir = atan2(vy, vx) * 180 / pi,
      dir = (dir + 360) %% 360,
      x_end_rel = s * cos((90 - dir) * pi / 180) + x_rel,
      y_end = s * sin((90 - dir) * pi / 180) + y,
      turn_angle = atan2(
        lead(y) - y,
        lead(x_rel) - x_rel
      ) -
        atan2(
          y - dplyr::lag(y),
          x_rel - dplyr::lag(x_rel)
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

#' Prepped input and output is split due to the different schemas.
#' Could be abstracted to a single loadff
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
    standardize_tracking_directions()
}

get_prepped_output <- function(con, weeks = NULL) {
  x <- tbl(con, "tracking_output")
  if (all(is.numeric(weeks))) {
    cli::cli_inform(glue::glue("Filtering for week: {weeks}"))
    x <- x |> filter(week %in% weeks)
  }

  x |>
    standardize_tracking_directions()
}

#' in R because of `add_tracking_features()`
prep_data <- function(con, week = NULL) {
  x <- get_prepped_data(con, week) |>
    group_by(game_id, play_id) |>
    mutate(min_frame = min(frame_id)) |>
    ungroup() |>
    mutate(
      frame_id = 1 + (frame_id - min_frame)
    ) |>
    collect()
  y <- get_prepped_output(con, week) |>
    group_by(game_id, play_id) |>
    mutate(min_frame = min(frame_id)) |>
    ungroup() |>
    mutate(
      frame_id = 1 + (frame_id - min_frame)
    ) |>
    collect()

  x |>
    select(any_of(colnames(y))) |>
    mutate(dataset = 'X') |>
    bind_rows(y |> mutate(dataset = 'Y')) |>
    group_by(game_id, play_id, nfl_id) |>
    arrange(dataset, frame_id, .by_group = TRUE) |>
    mutate(frame_id2 = row_number()) |>
    # -----  KEY ---------
    # test out only keeping every 3 plays
    # testing toggleing this off
    filter(frame_id2 %% 3 == 0) |>
    add_tracking_features() |>
    ungroup() |>
    inner_join(
      x |> distinct(game_id, play_id, ball_land_x_rel, ball_land_y)
    ) |>
    inner_join(
      x |> distinct(game_id, play_id, nfl_id, player_side, player_position)
    )
}

cli::cli_alert_info("creating combined_data")
dbWriteTable(con, "combined_data", prep_data(con), overwrite = TRUE)
cli::cli_alert_info("creating distance dataset")


#' this is a dataset that gets the start of a play, where the players on offense
#' besides the QB have all had a speed of 1 or greater

tbl(con, "combined_data") |>
  filter(player_side == 'Offense', player_position != 'QB') |>
  mutate(s_filter = s > 1) |>
  filter(s_filter) |>
  group_by(game_id, play_id, nfl_id) |>
  summarise(min_frame = min(frame_id2), .groups = 'drop') |>
  group_by(game_id, play_id) |>
  summarise(frame_start = max(min_frame), .groups = 'drop') |>
  compute(name = 'play_starts', overwrite = TRUE)

#' ------- Start Distance datasets -----------------
offensive <- tbl(con, "combined_data") |>
  inner_join(tbl(con, "play_starts")) |>
  filter(player_side == 'Offense', dataset == 'X', player_position != 'QB') |>
  # this would be used if I wanted to get end of play distances
  # group_by(game_id, play_id, nfl_id) |>
  # arrange(frame_id2, .by_group = TRUE) |>
  # mutate(rn = row_number()) |>
  select(
    game_id,
    play_id,
    off_id = nfl_id,
    frame_id2,
    off_x = x_rel,
    off_y = y
  )

defensive <- tbl(con, "combined_data") |>
  filter(player_side == 'Defense') |>
  inner_join(tbl(con, "play_starts")) |>
  select(
    game_id,
    play_id,
    def_id = nfl_id,
    frame_id2,
    def_x = x_rel,
    def_y = y
  )

distances <- offensive |>
  inner_join(defensive, by = c('game_id', 'play_id', 'frame_id2')) |>
  mutate(
    distance = sqrt((off_x - def_x)^2 + (off_y - def_y)^2)
  ) |>
  group_by(game_id, play_id, frame_id2, off_id) |>
  dbplyr::window_order(distance) |>
  mutate(r = row_number()) |>
  ungroup()


distances |>
  left_join(tbl(con, "play_starts")) |>
  filter(frame_id2 >= frame_start) |>
  group_by(game_id, play_id) |>
  mutate(max_frame = max(frame_id2)) |>
  ungroup() |>
  filter(frame_id2 > max_frame / 2) |>
  left_join(
    tbl(con, "combined_data") |>
      select(game_id, play_id, off_id = nfl_id, vx, vy, frame_id2)
  ) |>
  left_join(
    tbl(con, "combined_data") |>
      select(
        game_id,
        play_id,
        def_id = nfl_id,
        def_vx = vx,
        def_vy = vy,
        frame_id2
      )
  ) |>
  compute(name = 'distances', overwrite = TRUE)


#' AFter Distances, rough coverage assignment. Could be improved with a model,
#' but for now it is linear calculations based on proximity and direction
tbl(con, "distances") |>
  mutate(
    mag_receiver = sqrt(vx^2 + vy^2),
    mag_defender = sqrt(def_vx^2 + def_vy^2),
    dot_prod = vx * def_vx + vy * def_vy,
    velocity_alignment = dot_prod / (mag_receiver * mag_defender),
    velocity_alignment = ifelse(
      mag_receiver < 0.1 | mag_defender < 0.1,
      NA_real_,
      velocity_alignment
    ),
    distance_score = exp(-distance / 5),
    alignment_score = (velocity_alignment + 1) / 2,
    coverage_score = distance_score * alignment_score,
    is_close = as.numeric(distance < 8)
  ) |>
  select(-starts_with('mag'), -dot_prod) |>
  group_by(game_id, play_id, off_id, def_id) |>
  summarise(
    pct_close = mean(is_close, na.rm = TRUE),
    mean_coverage_score = mean(coverage_score, na.rm = TRUE),
    max_coverage_score = max(coverage_score, na.rm = TRUE),
    mean_distance = mean(distance),
    .groups = 'drop'
  ) |>
  inner_join(
    tbl(con, "players") |> select(off_id = nfl_id, off_name = player_name)
  ) |>
  inner_join(
    tbl(con, "players") |> select(def_id = nfl_id, def_name = player_name)
  ) |>
  #collect() |>
  group_by(game_id, play_id, off_id) |>
  dbplyr::window_order(desc(max_coverage_score)) |>
  mutate(
    max_cs_delta = max_coverage_score - lead(max_coverage_score),
    coverage_rank = rank(desc(max_coverage_score)) #, ties.method = "first")
  ) |>
  ungroup() |>
  filter(
    coverage_rank <= 1,
    mean_coverage_score > 0.2, # CHANGED - can use higher threshold now
    mean_distance < 8, # Keep mean distance constraint
    pct_close > 0.5,
    max_cs_delta > 0.2
  ) |>
  select(
    game_id,
    play_id,
    off_id,
    off_name,
    def_id,
    def_name,
    mean_coverage_score,
    max_coverage_score,
    max_cs_delta
  ) |>
  compute(name = "coverage_assignments", overwrite = TRUE)


# test_assignments <- tbl(con, "coverage_assignments") |> .filter() |> collect()
# assertthat::are_equal(
#   all(sort(test_assignments$def_id) == c(52458, 54719, 55921)),
#   TRUE
# )

# df_final_final_final
tbl(con, "combined_data") |>
  filter(player_position %in% c('TE', 'RB', 'WR')) |>
  collect() |>
  inner_join(
    tbl(con, "coverage_assignments") |>
      select(game_id, play_id, nfl_id = off_id, off_name, def_id, def_name) |>
      collect()
  ) |>
  arrange(frame_id2) |>
  left_join(
    tbl(con, "combined_data") |>
      select(
        game_id,
        play_id,
        def_x = x_rel,
        def_y = y,
        def_s = s,
        def_id = nfl_id,
        frame_id2,
        def_turn_angle = turn_angle,
        prev_def_turn_angle = prev_angle,
        def_vx = vx,
        def_vy = vy
      ) |>
      collect()
  ) |>
  mutate(
    angle_with_rec = atan2((y - def_y), (x_rel - def_x))
  ) |>
  mutate(vangle = atan2(vx, vy), def_vangle = atan2(def_vx, def_vy)) |>
  group_by(game_id, play_id, nfl_id) |>
  arrange(frame_id2, .by_group = TRUE) |>
  mutate(
    # delta_o = vangle - lag(vangle),
    # delta_d = def_vangle - lag(def_vangle),
    # delta_o = delta_o / 0.10,
    # delta_d = delta_d / 0.10,
    # delta = delta_o - delta_d,
    distance = sqrt((x_rel - def_x)^2 + (y - def_y)^2),
    player_distance = sqrt((x_rel - lag(x_rel))^2 + (y - lag(y))^2),
    diff_turn_angle = round(
      atan2(
        sin(turn_angle - def_turn_angle),
        cos(turn_angle - def_turn_angle)
      ),
      3
    )
  ) |>
  ungroup() |>
  inner_join(
    tbl(con, "play_starts") |> collect(),
    by = c('game_id', 'play_id')
  ) |>
  group_by(game_id, play_id, nfl_id) |>
  arrange(frame_id2, .by_group = TRUE) |>
  filter(!is.na(player_distance)) |>
  mutate(off_cum_distance = cumsum(player_distance)) |>
  filter(frame_id2 > frame_start) |>
  left_join(
    get_supplementary_data() |> select(game_id, play_id, team_coverage_man_zone)
  ) |>
  filter(!is.na(team_coverage_man_zone)) |>
  group_by(game_id, play_id, nfl_id) |>
  arrange(frame_id2, .by_group = TRUE) |>
  mutate(
    frame_id = row_number(),
    diff_turn_angle = ifelse(
      frame_id == 1 & abs(diff_turn_angle) > lead(diff_turn_angle) * 2,
      NA,
      diff_turn_angle
    )
  ) |>
  ungroup() |>
  arrow::write_parquet('analytics/prepped_data/prepped_data.parquet')

dbDisconnect(con)
