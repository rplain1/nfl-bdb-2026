library(tidyverse)
library(duckdb)
source('analytics/prep_data.R')
source('analytics/distance_data.R')
source('analytics/gmm.R')
con <- dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

prep_data(con, load_data = FALSE)
load_distance_data(con)
run_gmm_model(con)


play_filter <- function(.data, play) {
  dbWriteTable(con, "play", play, overwrite = TRUE)

  inner_join(.data, tbl(con, "play"))
}


play <- tibble(
  game_id = c(2023092408),
  play_id = c(1574),
  player_name = 'Trent Sherfield',
  player_name_def = 'Benjamin St-Juste'
) # trent Sherfield


get_defense_players <- function(df) {
  df |>
    group_by(player_name) |>
    filter(frame_id == max(frame_id) - 5) |>
    filter(distance == min(distance)) |>
    select(player_name_def)
}

get_wrong_defense_players <- function(df) {
  df |>
    group_by(player_name) |>
    filter(frame_id == max(frame_id) - 5) |>
    filter(distance == max(distance)) |>
    select(player_name_def) |>
    anti_join(get_defense_players(df))
}

get_relevant_defenders <- function() {
  tbl(con, "distances") |>
    filter(dataset == 'Y') |>
    summarise(
      avg_distance = avg(distance),
      .by = c(
        game_id,
        play_id,
        nfl_id,
        nfl_id_def,
        player_name_def,
        player_name
      )
    ) |>
    group_by(game_id, play_id, nfl_id) |>
    filter(avg_distance == min(avg_distance)) |>
    ungroup()
}


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

tbl(con, "distances") |>
  play_filter(play) |>
  collect() -> df

df |>
  arrange(game_id, play_id, nfl_id, nfl_id_def, frame_id) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
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
  ungroup() |>
  inner_join(get_defense_players(df)) |>
  filter(frame_id > 8) -> df_plot


df_plot |>
  #filter(player_name == 'Marvin Jones') |>
  select(
    frame_id,
    nfl_id,
    nfl_id_def,
    player_name,
    player_name_def,
    diff_turn_angle,
    angle_with_def,
    angle_with_def_diff,
    angle_with_def_diff_mag,
    angle_with_def_diff_sign,
    distance
  ) |>
  group_by(nfl_id, nfl_id_def) |>
  mutate(
    angle_changepoint = ifelse(
      angle_with_def_diff_sign != lag(angle_with_def_diff_sign),
      1,
      0
    )
  ) |>
  filter(!is.na(angle_changepoint)) |>
  mutate(
    z_angle = round(
      (angle_with_def_diff_mag - mean(angle_with_def_diff_mag, na.rm = TRUE)) /
        sd(angle_with_def_diff_mag, na.rm = TRUE),
      3
    ),
    angle = cumsum(angle_with_def_diff_sign),
    abs_diff = abs(diff_turn_angle),
    z = (abs_diff - mean(abs_diff, na.rm = TRUE)) / sd(abs_diff, na.rm = TRUE),
  ) |>
  # mutate(
  #   win = abs(z) > 3 &
  # )
  print(n = 35)


tbl(con, "distances") |>
  #inner_join(tbl(con, "play_starts")) |>
  inner_join(get_relevant_defenders()) |>
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
  ungroup() |>
  filter(frame_id > frame_start + 2) |>
  collect() -> df

df |>
  inner_join(play) |>
  select(
    frame_id,
    nfl_id,
    nfl_id_def,
    player_name,
    player_name_def,
    x,
    y,
    o,
    a,
    s_mph,
    x_def,
    y_def,
    o_def,
    a_def,
    s_mph_def,
    angle_with_def,
    angle_with_def_diff,
    angle_with_def_diff_mag,
    diff_turn_angle
  ) |>
  group_by(nfl_id, nfl_id_def) |>
  mutate(
    z_angle = round(
      (angle_with_def_diff_mag - mean(angle_with_def_diff_mag, na.rm = TRUE)) /
        sd(angle_with_def_diff_mag, na.rm = TRUE),
      3
    ),
    abs_diff = abs(diff_turn_angle),
    z = (abs_diff - mean(abs_diff, na.rm = TRUE)) / sd(abs_diff, na.rm = TRUE),
    z_abs = abs(z),
    .filter = cumsum(z_abs > 3),
    cum_angle_def_diff_mag = angle_with_def_diff_mag -
      cummean(angle_with_def_diff_mag)
  ) |>
  mutate(across(where(is.numeric), ~ round(.x, 3))) |>
  filter(player_name == 'Mike Evans') |>
  print(n = Inf)
group_by(player_name, player_name_def) |>
  summarise(
    frame_id = min(frame_id),
    across(
      starts_with('z'),
      list(min = ~ min(., na.rm = TRUE), max = ~ max(., na.rm = TRUE))
    )
  )


df_plot |>
  select(
    frame_id,
    player_name,
    diff_turn_angle,
    #angle_with_def,
    angle_with_def_diff,
    angle_with_def_diff_mag,
    distance
  ) |>
  pivot_longer(-c(frame_id, player_name)) |>
  ggplot(aes(frame_id, value, color = player_name)) +
  geom_line() +
  facet_wrap(~name, scales = 'free_y', ncol = 1)
