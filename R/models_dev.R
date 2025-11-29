library(torch)
library(tidyverse)
# Load raw data

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
train_dl <- dataloader(
  torch_load('datasets/train_ds.pt'),
  batch_size = 16,
  shuffle = TRUE
)
val_dl <- dataloader(
  torch_load('datasets/val_ds.pt'),
  batch_size = 16,
  shuffle = TRUE
)
trajectory_transformer <- nn_module(
  classname = "trajectory_transformer",
  initialize = function(
    num_features = 8,
    d_model = 128,
    nhead = 8,
    num_encoder_layers = 4,
    dim_feedforward = 512,
    dropout = 0.1,
    max_players = 22,
    max_frames = 100
  ) {
    self$d_model <- d_model
    self$max_players <- max_players
    self$max_frames <- max_frames

    # Input projection: features -> d_model
    self$feature_projection <- nn_linear(num_features, d_model)

    # Learnable positional embeddings
    self$spatial_pos_embed <- nn_embedding(max_players, d_model)
    self$temporal_pos_embed <- nn_embedding(max_frames, d_model)

    # Transformer encoder
    encoder_layer <- nn_transformer_encoder_layer(
      d_model = d_model,
      nhead = nhead,
      dim_feedforward = dim_feedforward,
      dropout = dropout,
      batch_first = TRUE
    )

    self$transformer_encoder <- nn_transformer_encoder(
      encoder_layer = encoder_layer,
      num_layers = num_encoder_layers
    )

    # Output head
    self$output_head <- nn_sequential(
      nn_linear(d_model, d_model),
      nn_relu(),
      nn_dropout(dropout),
      nn_linear(d_model, 2) # (x, y) coordinates
    )

    self$dropout <- nn_dropout(dropout)
  },
  forward = function(x, frame_mask = NULL, player_mask = NULL) {
    # Check actual dimensions
    B <- dim(x)[[1]]
    dim2 <- dim(x)[[2]]
    dim3 <- dim(x)[[3]]

    # Determine if we need to transpose based on which dimension is larger
    # Frames should typically be more than players in football tracking
    if (dim2 > dim3) {
      # x is [B, F, P, num_features]
      F <- dim2
      P <- dim3
    } else {
      # x is [B, P, F, num_features] - need to transpose
      P <- dim2
      F <- dim3
      x <- x$permute(c(1, 3, 2, 4)) # [B, F, P, num_features]
    }

    # Project features to d_model
    x <- self$feature_projection(x) # [B, F, P, d_model]

    # Add positional embeddings
    # Temporal positions: one position per frame
    temporal_pos <- torch_arange(
      0,
      F - 1,
      device = x$device,
      dtype = torch_long()
    ) # [F]
    temporal_embed <- self$temporal_pos_embed(temporal_pos + 1L) # [F, d_model]
    # Reshape to [1, F, 1, d_model] for broadcasting
    temporal_embed <- temporal_embed$view(c(1, F, 1, self$d_model))

    # Spatial positions: one position per player
    spatial_pos <- torch_arange(
      0,
      P - 1,
      device = x$device,
      dtype = torch_long()
    ) # [P]
    spatial_embed <- self$spatial_pos_embed(spatial_pos + 1L) # [P, d_model]
    # Reshape to [1, 1, P, d_model] for broadcasting
    spatial_embed <- spatial_embed$view(c(1, 1, P, self$d_model))

    # Add both positional embeddings (broadcasting will handle the expansion)
    x <- x + temporal_embed + spatial_embed
    x <- self$dropout(x)

    # Reshape to sequence: flatten frames and players
    x <- x$view(c(B, F * P, self$d_model)) # [B, F*P, d_model]

    # Create attention mask if provided
    attn_mask <- NULL
    if (!is.null(frame_mask) && !is.null(player_mask)) {
      # Expand masks to cover all frame-player combinations
      frame_mask_exp <- frame_mask$unsqueeze(3)$expand(c(-1, -1, P)) # [B, F, P]
      player_mask_exp <- player_mask$unsqueeze(2)$expand(c(-1, F, -1)) # [B, F, P]

      # Combined mask: both frame and player must be valid
      combined_mask <- frame_mask_exp & player_mask_exp # [B, F, P]
      attn_mask <- combined_mask$view(c(B, F * P)) # [B, F*P]

      # Invert mask for transformer (True = ignore)
      attn_mask <- !attn_mask
    }

    # Apply transformer encoder
    if (!is.null(attn_mask)) {
      x <- self$transformer_encoder(x, src_key_padding_mask = attn_mask)
    } else {
      x <- self$transformer_encoder(x)
    }

    # Reshape back to [B, F, P, d_model]
    x <- x$view(c(B, F, P, self$d_model))

    # Predict (x, y) coordinates
    output <- self$output_head(x) # [B, F, P, 2]

    return(output)
  }
)

train_model <- function(
  model,
  train_dl,
  val_dl,
  epochs = 1,
  lr = 0.001,
  device = "cpu"
) {
  optimizer <- optim_adam(model$parameters, lr = lr)

  # Move model to device
  model$to(device = device)
  model$train()

  for (epoch in seq_len(epochs)) {
    train_loss <- 0
    train_batches <- 0

    coro::loop(
      for (batch in train_dl) {
        # Move batch to device
        batch$x <- batch$x$to(device = device)
        batch$y <- batch$y$to(device = device)
        batch$frame_mask <- batch$frame_mask$to(device = device)
        batch$player_mask <- batch$player_mask$to(device = device)
        batch$target_mask <- batch$target_mask$to(device = device)

        optimizer$zero_grad()

        # Pass masks to model
        pred <- model(
          batch$x,
          frame_mask = batch$frame_mask,
          player_mask = batch$player_mask
        )

        # Calculate masked loss
        # batch$x shape: [B, F, P, features]
        # pred, batch$y shape: [B, F, P, 2]
        # masks: frame_mask [B, F], player_mask [B, P], target_mask [B, P]

        # Expand masks to match prediction dimensions [B, F, P, 2]
        frame_mask_exp <- batch$frame_mask$unsqueeze(3)$unsqueeze(4) # [B, F, 1, 1]
        player_mask_exp <- batch$player_mask$unsqueeze(2)$unsqueeze(4) # [B, 1, P, 1]
        target_mask_exp <- batch$target_mask$unsqueeze(2)$unsqueeze(4) # [B, 1, P, 1]

        # Combine all masks
        mask <- (frame_mask_exp & player_mask_exp & target_mask_exp)$to(
          dtype = torch_float()
        )

        # Compute MSE loss only on valid positions
        diff <- (pred - batch$y)^2
        loss <- (diff * mask)$sum() / (mask$sum() + 1e-8) # Add epsilon to avoid division by zero

        loss$backward()
        optimizer$step()

        train_loss <- train_loss + loss$item()
        train_batches <- train_batches + 1
      }
    )

    # Validation
    model$eval()
    val_loss <- 0
    val_batches <- 0

    with_no_grad({
      coro::loop(
        for (batch in val_dl) {
          # Move batch to device
          batch$x <- batch$x$to(device = device)
          batch$y <- batch$y$to(device = device)
          batch$frame_mask <- batch$frame_mask$to(device = device)
          batch$player_mask <- batch$player_mask$to(device = device)
          batch$target_mask <- batch$target_mask$to(device = device)

          pred <- model(
            batch$x,
            frame_mask = batch$frame_mask,
            player_mask = batch$player_mask
          )

          # Same masking logic
          frame_mask_exp <- batch$frame_mask$unsqueeze(3)$unsqueeze(4)
          player_mask_exp <- batch$player_mask$unsqueeze(2)$unsqueeze(4)
          target_mask_exp <- batch$target_mask$unsqueeze(2)$unsqueeze(4)

          mask <- (frame_mask_exp & player_mask_exp & target_mask_exp)$to(
            dtype = torch_float()
          )

          diff <- (pred - batch$y)^2
          loss <- (diff * mask)$sum() / (mask$sum() + 1e-8)

          val_loss <- val_loss + loss$item()
          val_batches <- val_batches + 1
        }
      )
    })

    cat(sprintf(
      "Epoch %d - Train Loss: %.4f, Val Loss: %.4f\n",
      epoch,
      train_loss / train_batches,
      val_loss / val_batches
    ))

    model$train()
  }

  return(model)
}

model <- train_model(
  trajectory_transformer(),
  train_dl,
  val_dl,
  epochs = 10,
  lr = 0.001,
  device = "cpu"
)
torch_save(model, "models/transformer_model.pt")
