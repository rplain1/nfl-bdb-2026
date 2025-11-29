dbDisconnect(con)

library(brms)


model_df <- df |>
  filter(player_position == 'WR') |>
  filter(!is.na(turn_angle), !is.na(diff_turn_angle)) |>
  select(
    game_id,
    play_id,
    nfl_id,
    frame_id,
    prev_angle,
    contains("turn_angle"),
    x_rel,
    y,
    def_x,
    def_y,
    distance,
    s,
    def_s,
    player_position,
    off_cum_distance,
    team_coverage_man_zone
  )

fit <- brm(
  bf(
    diff_turn_angle ~
      frame_id +
      def_turn_angle +
      turn_angle +
      prev_def_turn_angle +
      prev_angle +
      x_rel +
      y +
      def_x +
      def_y +
      distance +
      def_s +
      team_coverage_man_zone,

    kappa ~ off_cum_distance + s + (1 | nfl_id),
    decomp = 'QR'
  ),
  family = von_mises(),
  chains = 4,
  iter = 3500,
  warmup = 1500,
  seed = 52723,
  cores = 4,
  init = "0",
  control = list(adapt_delta = 0.9),
  backend = "cmdstanr",
  data = model_df
)
