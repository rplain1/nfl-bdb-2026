library(torch)
library(tidyverse)
library(luz)
source('R/dataset.R')
create_dataloaders()


get_device <- function() {
  if (torch::cuda_is_available()) {
    torch_device("cuda")
  } else if (torch::backends_mps_is_available()) {
    torch_device("mps")
  } else {
    torch_device("cpu")
  }
}


# Load raw data
train_data <- torch_load('datasets/train_ds.pt')
val_data <- torch_load('datasets/val_ds.pt')

train_dl <- dataloader(train_data, batch_size = 16, shuffle = TRUE)
val_dl <- dataloader(val_data, batch_size = 16, shuffle = TRUE)


trajectory_baseline <- nn_module(
  classname = "trajectory_baseline",
  initialize = function(
    num_features,
    hidden_dim = 64,
    output_dim = 2,
    dropout = 0.1
  ) {
    # Embed player features
    self$feature_embed <- nn_sequential(
      nn_linear(num_features, hidden_dim),
      nn_relu(),
      nn_dropout(dropout)
    )

    # Temporal modeling with a GRU (sequence along frames)
    self$gru <- nn_gru(
      input_size = hidden_dim,
      hidden_size = hidden_dim,
      batch_first = TRUE
    )

    # Output head for (x,y)
    self$output_head <- nn_sequential(
      nn_linear(hidden_dim, hidden_dim),
      nn_relu(),
      nn_linear(hidden_dim, output_dim)
    )
  },

  forward = function(x, frame_mask = NULL, player_mask = NULL) {
    # x: [B, F, P, num_features]

    B <- dim(x)[[1]]
    F <- dim(x)[[2]]
    P <- dim(x)[[3]]

    # Flatten players into batch dimension for sequence modeling
    x <- x$view(c(B * P, F, -1)) # [B*P, F, num_features]

    # Embed features
    x <- self$feature_embed(x) # [B*P, F, hidden_dim]

    # GRU over frames
    out <- self$gru(x) # out[[1]]: [B*P, F, hidden_dim]

    # Predict (x, y) for each frame
    preds <- self$output_head(out[[1]]) # [B*P, F, 2]

    # Reshape back to [B, F, P, 2]
    preds <- preds$view(c(B, P, F, 2))$permute(c(1, 3, 2, 4))

    return(preds) # [B, F, P, 2]
  }
)

train_model <- function(
  model,
  train_dl,
  val_dl,
  epochs = 1,
  lr = 0.001 #,
  #device = get_device()
) {
  optimizer <- optim_adam(model$parameters, lr = lr)

  model$train()

  for (epoch in seq_len(epochs)) {
    train_loss <- 0
    train_batches <- 0

    coro::loop(
      for (batch in train_dl) {
        optimizer$zero_grad()

        pred <- model(batch$x)

        # Calculate masked loss
        mask <- batch$frame_mask$unsqueeze(-1)$unsqueeze(-1) *
          batch$player_mask$unsqueeze(2)$unsqueeze(-1) *
          batch$target_mask$unsqueeze(2)$unsqueeze(-1)

        diff <- (pred - batch$y)^2
        loss <- (diff * mask$to(dtype = torch_float()))$sum() / mask$sum()

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
          pred <- model(batch$x)

          mask <- batch$frame_mask$unsqueeze(-1)$unsqueeze(-1) *
            batch$player_mask$unsqueeze(2)$unsqueeze(-1) *
            batch$target_mask$unsqueeze(2)$unsqueeze(-1)

          diff <- (pred - batch$y)^2
          loss <- (diff * mask$to(dtype = torch_float()))$sum() / mask$sum()

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

# TRAINING
model <- trajectory_baseline(num_features = 8) #$to(device = device)
fitted_model <- train_model(model, train_dl, val_dl, epochs = 50)

fitted_model |> torch_save("models/model.pt")


test_data <- torch_load('datasets/test_ds.pt')
test_dl <- dataloader(test_data, batch_size = 4, shuffle = FALSE)


# v4
# Ensure required packages are loaded
fitted_model$eval()

all_preds_list <- list()
global_batch_idx <- 0 # Initialize a counter for the current global index

with_no_grad({
  coro::loop(
    for (batch in test_dl) {
      preds <- fitted_model(batch$x) # [B, F, P, 2]

      B <- dim(batch$x)[[1]]
      F <- dim(batch$x)[[2]]
      P <- dim(batch$x)[[3]]

      preds_array <- as.array(preds)
      frame_mask_array <- as.array(batch$frame_mask)
      player_mask_array <- as.array(batch$player_mask)

      # loop over the *current* batch
      for (b in seq_len(B)) {
        # 1. CALCULATE THE GLOBAL INDEX
        global_b <- global_batch_idx + b

        # 2. Use the GLOBAL INDEX to retrieve the key
        key <- test_data$keys[global_b, ] # contains game_id, play_id, mirrored

        # 3. Create the play data frame and FILTER for players to predict
        play_ids_df <- test_data$features %>%
          inner_join(key, by = c("game_id", "play_id", "mirrored")) %>%

          # ADD THIS CRUCIAL FILTER: Only keep players we need to predict
          dplyr::filter(player_to_predict == TRUE) %>%

          # Use distinct() to handle potential duplicates in the features table,
          # though your count() output suggests this particular play is clean (n=1).
          # Keep it for robustness.
          dplyr::distinct(nfl_id, frame_id, .keep_all = TRUE) %>%
          arrange(nfl_id, frame_id)

        # Now, extract the unique IDs from the FILTERED list
        unique_players <- sort(unique(play_ids_df$nfl_id))
        unique_frames <- sort(unique(play_ids_df$frame_id))

        # ... (The rest of the loop continues)

        # The logic below relies on unique_players and unique_frames being correctly
        # indexed to the player and frame dimensions of the prediction tensor.
        df <- expand.grid(
          frame = seq_along(unique_frames),
          player_idx = seq_along(unique_players)
        )

        # Assign predictions (which should now only cover the 'player_to_predict' subset
        # of the tensor's player dimension)
        df$x_pred <- as.vector(preds_array[
          b,
          1:length(unique_frames),
          1:length(unique_players),
          1
        ])
        df$y_pred <- as.vector(preds_array[
          b,
          1:length(unique_frames),
          1:length(unique_players),
          2
        ])

        # Filter valid frames/players
        valid_mask <- frame_mask_array[b, df$frame] &
          player_mask_array[b, df$player_idx]
        df <- df[valid_mask, ]

        # map player_idx to nfl_id
        df$nfl_id <- unique_players[df$player_idx]
        df$game_id <- key$game_id
        df$play_id <- key$play_id
        df$frame_id <- unique_frames[df$frame]

        # ADD THE 'mirrored' COLUMN
        df$mirrored <- key$mirrored

        all_preds_list <- append(
          all_preds_list,
          list(df[, c(
            "game_id",
            "play_id",
            "nfl_id",
            "frame_id",
            "x_pred",
            "y_pred",
            "mirrored"
          )])
        )
      }
      # 3. Update the global index counter *after* processing the batch
      global_batch_idx <- global_batch_idx + B
    }
  )
})

cat(sprintf("Final collected plays: %d\n", length(all_preds_list)))
df_test <- arrow::read_parquet('prepped_data/test_targets.parquet')
# Step 1: Prepare the predictions data (as you have it)
tidy_preds <- all_preds_list |>
  bind_rows() |>
  as_tibble()

# Step 2: Clean the test features data (df_test)
# The duplication must be here. Select the columns you need for the final table
# and use distinct() on all of them to enforce uniqueness.
df_test_clean <- df_test |>
  # Select key columns and any metadata columns you want to keep
  select(
    game_id,
    play_id,
    nfl_id,
    frame_id,
    mirrored,
    x,
    y,
    week,
    distance_to_goal,
    play_direction,
    x_rel,
    min_frame
  ) |>
  # The CRITICAL step: enforce uniqueness across all columns that define the row
  distinct()

# Step 3: Join the clean tables
df <- tidy_preds |>
  inner_join(
    df_test_clean,
    by = c("game_id", "play_id", "nfl_id", "frame_id", "mirrored")
  )

# Now, the count should return n=1
df |> count(game_id, play_id, nfl_id, frame_id, mirrored)


df |>
  inner_join(df_test |> distinct(game_id, play_id) |> slice_sample(n = 1)) |>
  ggplot(aes(color = as.factor(nfl_id))) +
  geom_point(aes(x_rel, y, alpha = frame_id, shape = 'actual')) +
  geom_point(aes(x_pred, y_pred, alpha = frame_id, shape = 'pred'))
