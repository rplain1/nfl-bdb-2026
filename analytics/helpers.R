library(tidyverse)
library(duckdb)
play_filter <- function(.data, play) {
  dbWriteTable(con, "play", play, overwrite = TRUE)

  inner_join(.data, tbl(con, "play"))
}


get_common_plays <- function() {
  tribble(
    ~game_id   , ~play_id , ~player_name      ,
    2023092408 ,     1574 , "Trent Sherfield" ,
    2023091706 ,     2583 , "Mike Evans"      ,
    2023091710 ,     3659 , "Garrett Wilson"  ,
    2023101506 ,     3367 , "Josh Downs"      ,
    2023092406 ,     2741 , "Mike Williams"   ,
    2023091706 ,     3512 , "Chase Claypool"  ,
    2023090700 ,      361 , "Marvin Jones"    ,
    2023100808 ,     3368 , "A.J. Brown"      ,
    2023090700 ,      101 , "Josh Reynolds"
  )
}
