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

# Prep for cumulative quantity calculations
positions_df <- positions_df %>%
  mutate(Delta_Quantity = case_when(
    is.na(Quantity) ~ 0,
    Side == "BUY" ~ Quantity,
    Side == "SELL" ~ -Quantity
  ))

# Calculate cumulative quantities
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(Cumulative_Quantity = cumsum(Delta_Quantity)) %>%
  ungroup()

# Prep for cash calculations
positions_df <- positions_df %>%
  mutate(Value = case_when(
    is.na(Value) ~ 0,
    Ticker == "CASH" ~ 0,
    Side == "BUY" ~ -Value,
    Side == "SELL" ~ Value
  ))

# Calculate cash effect each day
positions_df <- positions_df %>%
  group_by(Date) %>%
  mutate(Cash_Effect = if_else(
    Ticker == "CASH",
    sum(Value),
    0
  )) %>%
  ungroup()

# Calculate cumulative cash
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(Cumulative_Quantity = if_else(
    Ticker == "CASH",
    Cumulative_Quantity + cumsum(Cash_Effect),
    Cumulative_Quantity
  )) %>%
  ungroup() %>%
  select(Date, Ticker, Cumulative_Quantity, Price)

# Calculate daily value of each position
positions_df <- positions_df %>%
  mutate(Value = Cumulative_Quantity * Price)

# Plot value over time
ggplot(positions_df, aes(x = Date, y = Value, color = Ticker)) +
  geom_line() +
  labs(
    title = "Portfolio Value Over Time",
    x = "Date",
    y = "Value"
  )
ggsave(file.path(current_dir, "portfolio_value.png"), width = 10, height = 6)