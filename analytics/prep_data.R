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
    #convert_tracking_cortesian() |>
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
    add_tracking_features() |>
    ungroup() |>
    inner_join(
      x |> distinct(game_id, play_id, ball_land_x_rel, ball_land_y)
    ) |>
    inner_join(
      x |> distinct(game_id, play_id, nfl_id, player_side, player_position)
    )
}

df <- prep_data(con, week = 2)

play <- df |>
  filter(ball_land_x_rel > 5 & ball_land_x_rel < 30) |>
  distinct(game_id, play_id) |>
  slice_sample(n = 1) |>
  distinct(game_id, play_id) #_[1, c('game_id', 'play_id')]

df |>
  filter(game_id == play$game_id, play_id == play$play_id)

# splines::bs(df$y$x_rel |> round() |> unique()) |>
#   as_tibble() |>
#   mutate(rn = row_number()) |>
#   pivot_longer(-rn) |>
#   ggplot(aes(rn, value, color = name)) +
#   geom_line() +
#   facet_wrap(~name)

.plot_df <- df |>
  inner_join(play) |>
  mutate(
    .color = case_when(
      dataset == 'X' & player_side == 'Offense' ~ 'blue',
      dataset == 'X' & player_side == 'Defense' ~ 'red',
      dataset == 'Y' ~ 'green',
      TRUE ~ NA
    )
  )


.plot_df |>
  ungroup() |>
  #filter(!is.na(turn_angle), !is.na(prev_angle)) |>
  mutate(turn_angle = abs(turn_angle)) |>
  ggplot(
    aes(
      x_rel,
      y,
      alpha = s_mph,
      color = s_mph,
      group = nfl_id,
    ),
  ) +
  geom_point() +
  geom_text(
    aes(label = player_position, alpha = frame_id2),
    size = 2.4,
    color = 'black',
    data = \(x) x |> filter(frame_id == max(frame_id2))
  ) +
  geom_point(aes(ball_land_x_rel, ball_land_y), shape = 2, color = 'black') +
  #scale_color_gradient2(low='red', mid='black', high='blue') +
  geom_vline(xintercept = 0)

sup_data |>
  inner_join(play) |>
  glimpse()


df$x |>
  inner_join(df$y |> distinct(game_id, play_id, nfl_id)) |>
  group_by(game_id, play_id) |>
  summarise(n = n_distinct(player_side), .groups = 'drop') |>
  count(n) |>
  mutate(perc = nn / sum(nn))

df$x |>
  filter(player_to_predict) |>
  group_by(game_id, play_id, player_side) |>
  summarise(n = n_distinct(nfl_id), .groups = 'drop') |>
  pivot_wider(
    id_cols = game_id:play_id,
    names_from = player_side,
    values_from = n
  ) |>
  count(Defense, Offense) |>
  mutate(perc = n / sum(n) * 100)

# # A tibble: 9 × 4
#   Defense Offense     n     perc
#     <int>   <int> <int>    <dbl>
# 1       1       1  2822 20.0
# 2       2       1  4658 33.0
# 3       3       1  3118 22.1
# 4       4       1  1589 11.3
# 5       5       1   612  4.34
# 6       6       1   141  0.999
# 7       7       1    25  0.177
# 8       8       1     1  0.00709
# 9      NA       1  1142  8.09
