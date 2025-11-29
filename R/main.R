library(tidyverse)
library(duckdb)

con <- dbConnect(duckdb::duckdb())

source('R/prep_data.R')
prep_data(con, week = 1)
