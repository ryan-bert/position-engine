rm(list = ls())

suppressMessages({
  library(dplyr)
  library(readr)
  library(DBI)
  library(RPostgres)
  library(jsonlite)
  library(ggplot2)
})

############################ LOAD PRICE DATA ############################

# Load postgress credentials
current_dir <- dirname(sys.frame(1)$ofile)
credentials_path <- file.path("~/Documents/Credentials/Raspberry Pi/financial-database.json")
credentials <- fromJSON(credentials_path)

# Connect to the database
conn <- dbConnect(
  Postgres(),
  dbname = credentials$dbname,
  host = credentials$host,
  port = as.integer(credentials$port),
  user = credentials$user,
  password = credentials$password
)

# Load data from the database
etf_df <- dbGetQuery(conn, "SELECT * FROM etfs") %>% mutate(Date = as.Date(Date))
stocks_df <- dbGetQuery(conn, "SELECT * FROM equities") %>% mutate(Date = as.Date(Date))
crypto_df <- dbGetQuery(conn, "SELECT * FROM crypto") %>% mutate(Date = as.Date(Date))
index_df <- dbGetQuery(conn, "SELECT * FROM indices") %>% mutate(Date = as.Date(Date))
futures_df <- dbGetQuery(conn, "SELECT * FROM futures") %>% mutate(Date = as.Date(Date))
macros_df <- dbGetQuery(conn, "SELECT * FROM macros") %>% mutate(Date = as.Date(Date))

# Combine into single dataframe
assets_df <- etf_df %>%
  bind_rows(stocks_df) %>%
  bind_rows(crypto_df) %>%
  bind_rows(index_df) %>%
  bind_rows(futures_df) %>%
  bind_rows(macros_df)

# Select relevant columns
assets_df <- assets_df %>%
  select(Date, Ticker, Price)

########################## CALCULATE POSITIONS ##########################