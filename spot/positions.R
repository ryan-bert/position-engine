rm(list = ls())

suppressMessages({
  library(dplyr)
  library(readr)
  library(DBI)
  library(RPostgres)
  library(jsonlite)
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

# Load executed trades data
trades_df <- read_csv(file.path(current_dir, "trades.csv"), show_col_types = FALSE) %>%
  mutate(Date = as.Date(Date))

# Filter prices for correct date range and tickers
prices_df <- assets_df %>%
  filter(Date >= min(trades_df$Date)) %>%
  filter(Ticker %in% trades_df$Ticker)

# Add CASH to assets
cash_df <- data.frame(Date = unique(prices_df$Date), Ticker = "CASH", Price = 1)
prices_df <- bind_rows(prices_df, cash_df)

# Join trades to price data
positions_df <- prices_df %>%
  left_join(trades_df, by = c("Date", "Ticker"))

# Prepare for cumulative calculations
positions_df <- positions_df %>%
  mutate(Quantity = case_when(
    is.na(Quantity) ~ 0,
    Side == "BUY" ~ Quantity,
    Side == "SELL" ~ -Quantity
  ))

# Calculate cumulative quantities
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(Cumulative_Quantity = cumsum(Quantity)) %>%
  ungroup()