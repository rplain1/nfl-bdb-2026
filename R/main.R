library(tidyverse)
library(duckdb)

con <- dbConnect(duckdb::duckdb())

source('R/prep_data.R')
process_data(con, week = 2)
