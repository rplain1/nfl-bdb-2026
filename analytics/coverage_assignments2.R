library(tidyverse)
library(duckdb)
source('analytics/helpers.R')
con <- duckdb::dbConnect(duckdb::duckdb(), "analytics/prepped_data/db.duckdb")

distances <- tbl(con, "distances")
combined_data <- tbl(con, "combined_data")
ball_is_in_air_df <- combined_data |>
  filter(dataset == 'Y') |>
  summarise(ball_in_air = min(frame_id), .by = c(game_id, play_id))

sup_data <- tbl(con, "supplementary_data") |>
  select(
    game_id,
    play_id,
    team_coverage_type,
    distance_to_goal,
    yards_to_go,
    route_of_targeted_receiver,
    play_action,
    team_coverage_man_zone
  )


# # ----- Distance -------
# tbl(con, "pair_features") |>
#   left_join(
#     ball_is_in_air_df
#   ) |>
#   left_join(sup_data) |>
#   filter(team_coverage_man_zone != 'NA') |>
#   filter(frame_id == ball_in_air - 1) |>
#   group_by(team_coverage_man_zone, pred_coverage = round(pred_coverage, 1)) |>
#   summarise(
#     n = n(),
#     across(
#       distance,
#       list(min = min, mean = mean, median = median, max = max)
#     )
#   ) |>
#   collect() |>
#   arrange(team_coverage_man_zone, pred_coverage) |>
#   print(n = Inf)
# # 5 yards zone, 3 yards main

# # ----- Velocity heading -------
# tbl(con, "pair_features") |>
#   left_join(
#     ball_is_in_air_df
#   ) |>
#   left_join(sup_data) |>
#   filter(team_coverage_man_zone != 'NA') |>
#   mutate(y = y - (53.3 / 2), y_def = y_def - (53.3 / 2)) |>
#   group_by(game_id, play_id, nfl_id, nfl_id_def) |>
#   dbplyr::window_order(frame_id) |>
#   mutate(
#     delta_x = x_def - x,
#     delta_y = y_def - y,
#     rel_speed_toward = (x - x_def) * vx_def + (y - y_def) * vy_def,
#     mag_receiver = sqrt(vx^2 + vy^2),
#     mag_defender = sqrt(vx_def^2 + vy_def^2),
#     dot_prod = vx * vx_def + vy * vy_def,
#     velocity_alignment = dot_prod / (mag_receiver * mag_defender),
#   ) |>
#   filter(frame_id == ball_in_air - 1) |>
#   group_by(team_coverage_man_zone, pred_coverage = round(pred_coverage, 1)) |>
#   summarise(
#     n = n(),
#     across(
#       velocity_alignment,
#       list(
#         min = ~ quantile(.x, p = 0.1),
#         mean = mean,
#         median = median,
#         max = ~ quantile(.x, p = 0.9)
#       )
#     )
#   ) |>
#   collect() |>
#   arrange(team_coverage_man_zone, pred_coverage) |>
#   print(n = Inf)
# # 0.95 man, 0.9 zone

# # ------ angle diff --------
# tbl(con, "pair_features") |>
#   left_join(
#     ball_is_in_air_df
#   ) |>
#   left_join(sup_data) |>
#   filter(team_coverage_man_zone != 'NA') |>
#   filter(frame_id == ball_in_air - 1) |>
#   mutate(
#     heading_wr = atan2(vy, vx),
#     heading_db = atan2(vy_def, vx_def),

#     raw_diff = heading_wr - heading_db,
#     angle_diff = abs(atan2(sin(raw_diff), cos(raw_diff)))
#   ) |>
#   group_by(team_coverage_man_zone, pred_coverage = round(pred_coverage, 1)) |>
#   summarise(
#     n = n(),
#     across(
#       angle_diff,
#       list(
#         min = ~ quantile(.x, p = 0.1),
#         mean = mean,
#         median = median,
#         max = ~ quantile(.x, p = 0.9)
#       )
#     )
#   ) |>
#   collect() |>
#   arrange(team_coverage_man_zone, pred_coverage) |>
#   print(n = Inf)
# 0.25 man, 0.4 zone

### CREATE CLASSIFICATION

tbl(con, "distances") |>
  left_join(
    ball_is_in_air_df
  ) |>
  left_join(
    sup_data
  ) |>
  filter(team_coverage_man_zone != 'NA') |>
  mutate(y = y - (53.3 / 2), y_def = y_def - (53.3 / 2)) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  dbplyr::window_order(frame_id) |>
  mutate(
    delta_x = x_def - x,
    delta_y = y_def - y,
    rel_speed_toward = (x - x_def) * vx_def + (y - y_def) * vy_def,
    mag_receiver = sqrt(vx^2 + vy^2),
    mag_defender = sqrt(vx_def^2 + vy_def^2),
    dot_prod = vx * vx_def + vy * vy_def,
    velocity_alignment = dot_prod / (mag_receiver * mag_defender),
    heading_wr = atan2(vy, vx),
    heading_db = atan2(vy_def, vx_def),

    raw_diff = heading_wr - heading_db,
    angle_diff = abs(atan2(sin(raw_diff), cos(raw_diff)))
  ) |>
  ungroup() |>
  compute(name = 'distances_2', temporary = FALSE, overwrite = TRUE)

tbl(con, "distances_2") |>
  filter(between(frame_id, ball_in_air - 5, ball_in_air - 1)) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  mutate(
    frames_avail = n()
  ) |>
  ungroup() |>
  mutate(
    is_coverage_defender = case_when(
      team_coverage_man_zone == 'ZONE_COVERAGE' &
        angle_diff < 0.4 &
        velocity_alignment > 0.9 &
        distance < 5 ~ 1L,
      team_coverage_man_zone == 'MAN_COVERAGE' &
        angle_diff < 0.25 &
        velocity_alignment > 0.95 &
        distance < 3 ~ 1L,
      TRUE ~ 0L
    )
  ) |>
  group_by(game_id, play_id, nfl_id, nfl_id_def) |>
  summarise(
    is_def_perc = sum(is_coverage_defender) / max(frames_avail),

    .groups = 'drop'
  ) |>
  mutate(is_def_assign = ifelse(is_def_perc > 0.5, 1, 0)) |>
  compute(
    name = 'coverage_assignments_play',
    temporary = FALSE,
    overwrite = TRUE
  )

tbl(con, "distances_2") |>
  filter(player_to_predict, player_to_predict_def) |>
  inner_join(tbl(con, "coverage_assignments_play")) |>
  count(is_def_assign) |>
  mutate(perc = n / sum(n))

tbl(con, "distances_2") |>
  filter(player_to_predict, player_to_predict_def) |>
  mutate(
    player_position_def = case_when(
      player_position_def %in% c('ILB', 'MLB', 'OLB', 'LB') ~ 'LB',
      player_position_def %in% c('FS', 'S', 'SS') ~ 'S',
      player_position_def %in% c('DT', 'NT', 'DE') ~ 'DT',
      TRUE ~ player_position_def
    )
  ) |>
  filter(
    player_position %in% c('WR', 'TE', 'RB'),
    player_position_def %in% c('LB', 'S', 'DT', 'CB')
  ) |>
  left_join(tbl(con, "coverage_assignments_play")) |>
  collect() |>
  mutate(is_def_assign = as.factor(is_def_assign)) -> df_model


### Tidymodels wf

library(tidymodels)

# 1. Set up model with parameters to tune
xgb_model <- boost_tree(
  mode = "classification",
  trees = 1000, # can tune this too if desired
  tree_depth = tune(),
  min_n = tune(),
  loss_reduction = tune(),
  sample_size = tune(),
  mtry = tune(),
  learn_rate = tune()
) %>%
  set_engine("xgboost")

# xgb_model <- boost_tree(mode = "classification") %>%
#   set_engine("xgboost")

set.seed(52723)
dat_split <- initial_split(df_model, prop = 0.7, strata = is_def_assign)
dat_train <- training(dat_split)

dat_folds <- vfold_cv(dat_train, v = 5, strata = is_def_assign)

set.seed(52723)
xgb_grid <- grid_space_filling(
  tree_depth(range = c(3, 10)),
  min_n(range = c(2, 40)),
  loss_reduction(range = c(-10, 1.5)),
  sample_size = sample_prop(range = c(0.5, 1.0)),
  mtry(range = c(5, 15)),
  learn_rate(range = c(-3, -0.5)),
  size = 10 #
)


xgb_rec <-
  recipe(
    is_def_assign ~
      x +
      y +
      vx +
      vy +
      s +
      a +
      x_def +
      y_def +
      vx_def +
      vy_def +
      s_def +
      a_def +
      player_position +
      player_position_def +
      team_coverage_man_zone +
      route_of_targeted_receiver +
      velocity_alignment +
      angle_diff +
      distance_to_goal +
      yards_to_go +
      play_action +
      rel_speed_toward,
    data = dat_train
  ) %>%
  step_log(distance_to_goal, base = 10) %>%
  step_dummy(all_nominal_predictors()) |>
  step_normalize(all_numeric_predictors())


xgb_wf <- workflow() %>%
  add_model(xgb_model) %>%
  add_recipe(xgb_rec)
## TUNNEEEEE -----

# library(mirai)
# daemons(2)

xgb_tuned <- tune_grid(
  xgb_wf,
  resamples = dat_folds,
  grid = xgb_grid,
  metrics = metric_set(pr_auc, roc_auc, precision, recall),
  control = control_grid(save_pred = TRUE, verbose = TRUE)
)

xgb_tuned |>
  collect_metrics() |>
  filter(.metric %in% c('pr_auc', 'roc_auc')) |>
  arrange(-mean)


best_params <- select_best(xgb_tuned, metric = 'pr_auc')
saveRDS(best_params, 'analytics/prepped_data.best_params.rds')
best_params <- select_best(xgb_tuned)


final_wf <- finalize_workflow(xgb_wf, best_params)


set.seed(52723)
dat_split <- initial_split(df_model, prop = 0.7, strata = is_def_assign)
dat_train <- training(dat_split)
dat_test <- testing(dat_split)


xgb_fit <- final_wf |>
  fit(dat_train)
saveRDS(xgb_fit, 'analytics/prepped_data/xgb_mod.rds')

# Get class predictions AND probabilities
test_predictions <- xgb_fit %>%
  predict(dat_test) %>%
  bind_cols(
    xgb_fit %>% predict(dat_test, type = "prob")
  ) %>%
  bind_cols(dat_test)

# Calculate classification metrics
classification_metrics <- metric_set(
  roc_auc,
  pr_auc,
  precision,
  recall,
  f_meas
)

test_predictions %>%
  classification_metrics(truth = is_def_assign, estimate = .pred_class, .pred_1)

print(test_metrics)

# Confusion matrix
conf_mat(test_predictions, truth = is_def_assign, estimate = .pred_class) |>
  broom::tidy() |>
  mutate(perc = value / sum(value))

# Visualize confusion matrix
test_predictions %>%
  conf_mat(truth = win, estimate = .pred_class) %>%
  autoplot(type = "heatmap")

# ROC curve
test_predictions %>%
  roc_curve(truth = is_def_assign, .pred_1) %>%
  autoplot()


# For training metrics (check overfitting)
train_predictions <- xgb_fit %>%
  predict(dat_train) %>%
  bind_cols(
    xgb_fit %>% predict(dat_train, type = "prob")
  ) %>%
  bind_cols(dat_train)

train_metrics <- train_predictions %>%
  classification_metrics(truth = is_def_assign, estimate = .pred_class, .pred_1)

print(train_metrics)


train_predictions |>
  bind_rows(test_predictions) -> df_output


dbWriteTable(con, 'coverage_predictions', df_output, overwrite = T)


df_output |>
  #filter(frame_id >= 10) |>
  ggplot(aes(frame_id, z_turn, color = .pred_1 > 0.45)) +
  geom_point() +
  geom_hline(yintercept = 3, linetype = 'dashed') +
  geom_hline(yintercept = -3, linetype = 'dashed') +
  facet_wrap(~ paste(player_name, player_name_def, sep = '\n'))
