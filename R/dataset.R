library(torch)
library(tidyverse)


bdb2026_dataset <- dataset(
  name = "bdb2026_dataset",
  initialize = function(features, targets, max_players = 22, num_features = 8) {
    self$features <- features
    self$targets <- targets
    self$keys <- unique(features[, c("game_id", "play_id", "mirrored")])

    self$max_frames <- max(features$frame_id)
    self$max_players <- max_players
    self$num_features <- num_features
  },

  .length = function() {
    nrow(self$keys)
  },

  .getitem = function(i) {
    key <- self$keys[i, ]

    # --- features ---
    play_df <- self$features %>%
      inner_join(key, by = c("game_id", "play_id", "mirrored")) %>%
      arrange(nfl_id, frame_id) %>%
      select(
        nfl_id,
        player_to_predict,
        frame_id,
        x = x_rel,
        y,
        vx,
        vy,
        ox,
        oy,
        ball_land_x = ball_land_x_rel,
        ball_land_y
      )

    # Get unique sorted IDs for consistent ordering
    unique_players <- sort(unique(play_df$nfl_id))
    unique_frames <- sort(unique(play_df$frame_id))
    num_frames <- length(unique_frames)
    num_players <- length(unique_players)

    players_to_predict <- play_df %>%
      filter(player_to_predict == 1) %>%
      pull(nfl_id) %>%
      unique()

    # Reshape to array directly (more efficient than pivot_wider)
    feature_array <- array(
      0,
      dim = c(num_frames, num_players, self$num_features)
    )

    for (j in seq_len(nrow(play_df))) {
      frame_idx <- which(unique_frames == play_df$frame_id[j])
      player_idx <- which(unique_players == play_df$nfl_id[j])
      feature_array[frame_idx, player_idx, ] <- as.numeric(play_df[
        j,
        c("x", "y", "vx", "vy", "ox", "oy", "ball_land_x", "ball_land_y")
      ])
    }

    play_tensor <- torch_tensor(feature_array, dtype = torch_float())

    # Pad to max dimensions
    padded_tensor <- torch_zeros(c(
      self$max_frames,
      self$max_players,
      self$num_features
    ))
    padded_tensor[1:num_frames, 1:num_players, ] <- play_tensor

    # --- masks ---
    frame_mask <- torch_zeros(self$max_frames, dtype = torch_bool())
    frame_mask[1:num_frames] <- TRUE

    player_mask <- torch_zeros(self$max_players, dtype = torch_bool())
    player_mask[1:num_players] <- TRUE

    target_mask <- torch_zeros(self$max_players, dtype = torch_bool())
    for (p in players_to_predict) {
      player_idx <- which(unique_players == p)
      if (length(player_idx) > 0) {
        target_mask[player_idx] <- TRUE
      }
    }

    # --- targets ---
    play_df_tar <- self$targets %>%
      inner_join(key, by = c("game_id", "play_id", "mirrored")) %>%
      arrange(nfl_id, frame_id) %>%
      select(nfl_id, frame_id, x, y)

    # Same approach for targets
    target_array <- array(0, dim = c(num_frames, num_players, 2))

    for (j in seq_len(nrow(play_df_tar))) {
      frame_idx <- which(unique_frames == play_df_tar$frame_id[j])
      player_idx <- which(unique_players == play_df_tar$nfl_id[j])
      target_array[frame_idx, player_idx, ] <- as.numeric(play_df_tar[
        j,
        c("x", "y")
      ])
    }

    target_tensor <- torch_tensor(target_array, dtype = torch_float())

    padded_target <- torch_zeros(c(self$max_frames, self$max_players, 2))
    padded_target[1:num_frames, 1:num_players, ] <- target_tensor

    list(
      x = padded_tensor,
      y = padded_target,
      frame_mask = frame_mask,
      player_mask = player_mask,
      target_mask = target_mask
    )
  }
)

test_it <- function() {
  train_ds <- bdb2026_dataset(df, df_tar, max_players = 22)

  train_dl <- dataloader(train_ds, batch_size = 4, shuffle = TRUE)

  sample <- train_ds$.getitem(1)
  sample_df <- df |>
    filter(
      game_id == sample$game_id,
      play_id == sample$play_id,
      mirrored == sample$mirrored
    )
  assertthat::are_equal(
    sample$target_mask$sum()$item(),
    length(unique(sample_df[sample_df$player_to_predict, ]$nfl_id))
  )

  assertthat::are_equal(
    sample$player_mask$sum()$item(),
    length(unique(sample_df$nfl_id))
  )

  assertthat::are_equal(
    sample$frame_mask$sum()$item(),
    length(unique(sample_df$frame_id))
  )
}


create_dataloaders <- function(
  max_players = 17,
  output_dir = 'datasets',
  input_dir = 'prepped_data'
) {
  cli::cli_h3("Creating dataloader objects")
  purrr::walk(c('train', 'val', 'test'), function(dataset) {
    cli::cli_alert_info("Creating {dataset} dataloader")
    df <- arrow::read_parquet(
      glue::glue('{input_dir}/{dataset}_features.parquet')
    )
    df_tar <- arrow::read_parquet(
      glue::glue('{input_dir}/{dataset}_targets.parquet')
    )
    cli::cli_alert_info("{dataset} features dimensions: {dim(df)}.")
    cli::cli_alert_info("{dataset} targets dimensions: {dim(df_tar)}.")

    .ds <- bdb2026_dataset(df, df_tar, max_players = max_players)

    torch::torch_save(.ds, glue::glue('{output_dir}/{dataset}_ds.pt'))
    cli::cli_alert_info(
      "{dataset} dataloader written to {output_dir}/{dataset}.pt"
    )
  })
  cli::cli_alert_success("All datasets created!")
}
