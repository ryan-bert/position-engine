rm(list = ls())

suppressMessages({
  library(dplyr)
  library(tidyr)
  library(readr)
  library(DBI)
  library(RPostgres)
  library(jsonlite)
  library(ggplot2)
})

# Define paths
current_dir <- dirname(sys.frame(1)$ofile)
credentials_path <- file.path("~/Documents/Credentials/Raspberry Pi/financial-database.json")
trades_path <- file.path(current_dir, "trades.csv")

############################ LOAD PRICE DATA ############################

# Load postgress credentials
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
trades_df <- read_csv(trades_path, show_col_types = FALSE) %>%
  mutate(Date = as.Date(Date))

# Filter prices for correct date range and tickers
prices_df <- assets_df %>%
  filter(Date >= min(trades_df$Date)) %>%
  filter(Ticker %in% trades_df$Ticker)

# Add CASH to prices
cash_df <- data.frame(Date = unique(prices_df$Date), Ticker = "CASH", Price = 1)
prices_df <- bind_rows(prices_df, cash_df)

# Round prices to 4 decimal places to avoid floating point errors
prices_df <- prices_df %>%
  mutate(Price = round(Price, 4))

# Join trades to price data
positions_df <- prices_df %>%
  left_join(trades_df, by = c("Date", "Ticker"))

# Fill static data
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  fill(Asset_Class, .direction = "down") %>%
  fill(Asset_Class, .direction = "up") %>%
  fill(Multiplier, .direction = "down") %>%
  fill(Multiplier, .direction = "up") %>%
  ungroup()

# Prep for cumulative quantity calculations
positions_df <- positions_df %>%
  mutate(Trade_Qty = case_when(
    is.na(Trade_Qty) ~ 0,
    Action %in% c("BUY", "LONG") ~ Trade_Qty,
    Action %in% c("SELL", "SHORT") ~ -Trade_Qty
  ))

# Calculate positions
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(
    Position = cumsum(Trade_Qty),
  ) %>%
  ungroup()

################# MARK-TO-MARKET, CASH EFFECT & SLIPPAGE #################

# Calculate mark-to-market (MtM) PnL
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(MtM = lag(Position, default = 0) * (Price - lag(Price, default = first(Price))) * Multiplier) %>%
  ungroup()

# Calculate cash effect from trades (non-futures) and MtM (futures)
positions_df <- positions_df %>%
  mutate(Cash_Effect = case_when(
    Ticker == "CASH" ~ 0,
    !is.na(Action) & Asset_Class != "FUTURE" ~ -Trade_Qty * Price * Multiplier,
    Asset_Class == "FUTURE" ~ MtM,
    TRUE ~ 0
  ))

# Calculate slippage
positions_df <- positions_df %>%
  mutate(Slippage = case_when(
    Ticker == "CASH" ~ 0,
    Action %in% c("BUY", "LONG") ~ (Trade_Price - Price) * Trade_Qty * Multiplier,
    Action %in% c("SELL", "SHORT") ~ (Price - Trade_Price) * Trade_Qty * abs(Multiplier),
    TRUE ~ 0
  ))

# Calculate fees
positions_df <- positions_df %>%
  mutate(Fee = if_else(
    !is.na(Action) & Ticker != "CASH",
    -abs(Trade_Qty) * Price * 0.001,
    0
  ))

# Calculate total cash effect each day
positions_df <- positions_df %>%
  group_by(Date) %>%
  mutate(Cash_Effect = if_else(
    Ticker == "CASH",
    Cash_Effect + sum(Cash_Effect) + sum(Slippage) + sum(Fee),
    Cash_Effect
  )) %>%
  ungroup()

# Add cumulative cash effect from CASH
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(Position = if_else(
    Ticker == "CASH",
    Position + cumsum(Cash_Effect),
    Position
  )) %>%
  mutate(Cash_Effect = if_else(
    Ticker == "CASH",
    0,
    Cash_Effect
  ))

#################### CALCULATE TOTAL AND REALISED PnL ####################

# Calculate rolling total and realised PnL per ticker
positions_df <- positions_df %>%
  group_by(Ticker) %>%
  arrange(Date) %>%
  mutate(
    Total_PnL = cumsum(MtM + Slippage + Fee),
    Realised_PnL = cumsum(Cash_Effect + Slippage + Fee)
  )

# Select relevant columns
positions_df <- positions_df %>%
  select(Date, Ticker, Asset_Class, Price, Multiplier, Position, MtM, Cash_Effect, Slippage, Fee, Total_PnL, Realised_PnL)

# Calculate total profits and attribution per ticker
ticker_performance_df <- positions_df %>%
  group_by(Ticker) %>%
  summarise(
    Total_Cash_Effect = sum(Cash_Effect),
    Total_Slippage = sum(Slippage),
    Total_MtM = sum(MtM),
    Total_Fee = sum(Fee),
    Total_PnL = Total_MtM + Total_Slippage + Total_Fee,
    Realised_PnL = Total_Cash_Effect + Total_Slippage + Total_Fee
  ) %>%
  ungroup()

####################### VALUE AND NOTIONAL EXPOSURE #######################

# Calculate value (Futures value = 0)
positions_df <- positions_df %>%
  mutate(Value = case_when(
    Asset_Class == "FUTURE" ~ 0,
    TRUE ~ Price * Position * Multiplier
  ))

# Calculate notional exposure
positions_df <- positions_df %>%
  group_by(Date) %>%
  mutate(Notional_Exposure = case_when(
    Ticker == "CASH" & sum(abs(Position[Asset_Class == "FUTURE"])) > 0 ~ 0,
    TRUE ~ Price * Position * Multiplier
  )) %>%
  ungroup()

# View as a single portfolio over time
portfolio_df <- positions_df %>%
  group_by(Date) %>%
  summarise(
    Value = sum(Value),
    Notional_Exposure = sum(Notional_Exposure),
    Total_PnL = sum(Total_PnL),
    Realised_PnL = sum(Realised_PnL)
  ) %>%
  ungroup()

# Calculate leverage ratio
portfolio_df <- portfolio_df %>%
  mutate(Leverage = Notional_Exposure / Value)
