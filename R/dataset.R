library(torch)
library(tidyverse)

df <- arrow::read_parquet('prepped_data/train_features.parquet')
df_tar <- arrow::read_parquet('prepped_data/train_targets.parquet')

bdb2026_dataset <- torch::dataset(
  name = "bdb2026_dataset",
  initialize = function(features, targets) {
    self$features <- features
    self$targets <- targets
    self$keys <- unique(features[, c(
      'game_id',
      'play_id',
      'nfl_id',
      'mirrored'
    )])
    self$max_players <- 17
  },
  .length = function() {
    nrow(self$keys)
  },
  .getitem = function(i) {
    X <- self$features |>
      inner_join(self$keys[i, ])
    y <- self$features |>
      inner_join(self$keys[i, ])
  }
)


bdb2026_dataset <- dataset(
  name = "bdb2026_dataset",
  initialize = function(features, targets, max_players = 22, num_features = 6) {
    self$features <- features
    self$targets <- targets
    self$keys <- unique(features[, c("game_id", "play_id", "mirrored")])

    # Pre-compute these to avoid repeated calculations
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
      select(nfl_id, player_to_predict, frame_id, x = x_rel, y, vx, vy, ox, oy)

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
        c("x", "y", "vx", "vy", "ox", "oy")
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

    # NEW: Target mask - which players have valid targets
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

    # Return list
    list(
      x = padded_tensor, # [max_frames, max_players, num_features]
      y = padded_target, # [max_frames, max_players, 2]
      frame_mask = frame_mask, # [max_frames] - which frames are real
      player_mask = player_mask, # [max_players] - which players exist
      target_mask = target_mask, # [max_players] - which players have targets
      game_id = key$game_id, # Single numeric value
      play_id = key$play_id, # Single numeric value
      mirrored = key$mirrored # Single logical value
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
