library(torch)

torch_set_num_threads(6)

# Simple LSTM model for player trajectory prediction
player_lstm <- nn_module(
  "PlayerLSTM",

  initialize = function(
    input_size = 6,
    hidden_size = 128,
    num_layers = 2,
    output_size = 2,
    dropout = 0.2
  ) {
    self$lstm <- nn_lstm(
      input_size = input_size,
      hidden_size = hidden_size,
      num_layers = num_layers,
      dropout = dropout,
      batch_first = TRUE
    )

    self$fc <- nn_linear(hidden_size, output_size)
  },

  forward = function(x) {
    # x shape: [batch, max_frames, max_players, num_features]
    batch_size <- x$shape[1]
    max_frames <- x$shape[2]
    max_players <- x$shape[3]

    # Reshape: [batch * players, frames, features]
    x_reshaped <- x$permute(c(1, 3, 2, 4))$reshape(c(
      batch_size * max_players,
      max_frames,
      -1
    ))

    # LSTM
    lstm_out <- self$lstm(x_reshaped)[[1]]

    # Last frame output
    last_output <- lstm_out[, -1, ]

    # Predict (x, y)
    predictions <- self$fc(last_output)

    # Reshape back: [batch, players, 2]
    predictions <- predictions$reshape(c(batch_size, max_players, 2))

    return(predictions)
  }
)

# Training function
train_model <- function(
  model,
  train_dl,
  val_dl = NULL,
  epochs = 20,
  lr = 0.001
) {
  optimizer <- optim_adam(model$parameters, lr = lr)

  for (epoch in 1:epochs) {
    cli::cli_alert(epoch)
    # Training
    model$train()
    train_loss <- 0
    train_count <- 0

    coro::loop(
      for (batch in train_dl) {
        optimizer$zero_grad()

        # Forward pass
        pred <- model(batch$x)

        # batch$y is [batch, frames, players, 2] - take last frame
        y_target <- batch$y[, -1, , ] # [batch, players, 2]

        # Compute masked loss
        player_mask <- batch$player_mask # [batch, players]
        target_mask <- batch$target_mask # [batch, players]

        combined_mask <- player_mask & target_mask
        mask_expanded <- combined_mask$unsqueeze(3)$to(dtype = torch_float())

        squared_error <- (pred - y_target)^2
        masked_error <- squared_error * mask_expanded

        num_valid <- mask_expanded$sum()
        loss <- masked_error$sum() / num_valid

        # Backward pass
        loss$backward()
        optimizer$step()

        train_loss <- train_loss + loss$item()
        train_count <- train_count + 1
      }
    )

    avg_train_loss <- train_loss / train_count

    # Validation
    if (!is.null(val_dl)) {
      model$eval()
      valid_loss <- 0
      valid_count <- 0

      with_no_grad({
        coro::loop(
          for (batch in val_dl) {
            pred <- model(batch$x)

            # Same masking logic as training
            player_mask <- batch$player_mask
            target_mask <- batch$target_mask

            if (length(player_mask$shape) == 1) {
              player_mask <- player_mask$unsqueeze(1)$expand(c(
                -1,
                pred$shape[1]
              ))$t()
              target_mask <- target_mask$unsqueeze(1)$expand(c(
                -1,
                pred$shape[1]
              ))$t()
            }

            combined_mask <- player_mask & target_mask
            mask_expanded <- combined_mask$unsqueeze(3)$to(
              dtype = torch_float()
            )

            squared_error <- (pred - batch$y)^2
            masked_error <- squared_error * mask_expanded

            num_valid <- mask_expanded$sum()
            loss <- masked_error$sum() / num_valid

            valid_loss <- valid_loss + loss$item()
            valid_count <- valid_count + 1
          }
        )
      })

      avg_valid_loss <- valid_loss / valid_count
      cat(sprintf(
        "Epoch %d: train_loss=%.4f, valid_loss=%.4f\n",
        epoch,
        avg_train_loss,
        avg_valid_loss
      ))
    } else {
      cat(sprintf("Epoch %d: train_loss=%.4f\n", epoch, avg_train_loss))
    }
  }

  return(model)
}

# Usage:
# model <- player_lstm(input_size = 6, hidden_size = 128, num_layers = 2)
# fitted_model <- train_model(model, train_dl, val_dl, epochs = 20, lr = 0.001)

# Predictions:
# model$eval()
# predictions <- list()
# with_no_grad({
#   coro::loop(for (batch in test_dl) {
#     pred <- model(batch$x)
#     predictions <- c(predictions, list(pred))
#   })
# })

df <- arrow::read_parquet('prepped_data/train_features.parquet')
df_tar <- arrow::read_parquet('prepped_data/train_targets.parquet')

df_val <- arrow::read_parquet('prepped_data/val_features.parquet')
df_val_tar <- arrow::read_parquet('prepped_data/val_targets.parquet')

train_ds <- bdb2026_dataset(df, df_tar, max_players = 17)
train_dl <- dataloader(train_ds, batch_size = 4, shuffle = TRUE)


val_ds <- bdb2026_dataset(df_val, df_val_tar, max_players = 17)
val_dl <- dataloader(train_ds, batch_size = 4, shuffle = TRUE)

## Usage:
model <- player_lstm(input_size = 6, hidden_size = 128, num_layers = 2)
fitted_model <- train_model(model, train_dl, val_dl, epochs = 20, lr = 0.001)

# Predictions:
# model$eval()
# predictions <- list()
# with_no_grad({
#   coro::loop(for (batch in test_dl) {
#     pred <- model(batch$x)
#     predictions <- c(predictions, list(pred))
#   })
# })
