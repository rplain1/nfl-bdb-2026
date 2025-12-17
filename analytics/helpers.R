library(tidyverse)
library(duckdb)
play_filter <- function(.data, play) {
  dbWriteTable(con, "play", play, overwrite = TRUE)

  inner_join(.data, tbl(con, "play"))
}


get_common_plays <- function() {
  tribble(
    ~game_id   , ~play_id , ~player_name        ,
    2023092408 ,     1574 , "Trent Sherfield"   ,
    2023091706 ,     2583 , "Mike Evans"        ,
    2023091710 ,     3659 , "Garrett Wilson"    ,
    2023101506 ,     3367 , "Josh Downs"        ,
    2023092406 ,     2741 , "Mike Williams"     ,
    2023091706 ,     3512 , "Chase Claypool"    ,
    2023090700 ,      361 , "Marvin Jones"      ,
    #2023100808 ,     3368 , "A.J. Brown"      ,
    2023090700 ,      101 , "Josh Reynolds"     ,
    2023121011 ,     2721 , "Amon-Ra St. Brown" ,
    2024010713 ,      746 , "Stefon Diggs"
  )
}

plot_play <- function(play) {
  tbl(con, "combined_data") |>
    play_filter(play) |>
    collect() |>
    ggplot(aes(x, y)) +
    geom_point(aes(alpha = frame_id, color = dataset)) +
    geom_text(aes(label = player_name), size = 3, data = \(x) {
      x |> filter(frame_id == min(frame_id))
    })
}
