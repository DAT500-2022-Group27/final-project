library(sparklyr)
library(dplyr)

# read in data csv, rename some key fields, and format the date
ose_stocks <- read.csv(file="part-00000", header=FALSE)
names(ose_stocks)[names(ose_stocks) == "V1"] <- "Ticker"
names(ose_stocks)[names(ose_stocks) == "V2"] <- "DateTime"
names(ose_stocks)[names(ose_stocks) == "V6"] <- "ClosePrice"
ose_stocks[["DateTime"]] <- as.POSIXct(strptime(ose_stocks[["DateTime"]], format = "%Y-%m-%d %H:%M:%S", tz="Europe/Oslo"))


# Function to compute SVD for an individual stock
run_svd <- function(stock_df) {
  library(dplyr)
  # all columns going to SVD begin with V e.g. "V4","V11"
  svd_fields <- stock_df %>%
    select(starts_with("V"))
  svd_fields[is.na(svd_fields)] <- 0 # replace NaNs with 0 (appears to be mostly from SD of period with only 1 record)
  
  stock_df <- stock_df %>%
    select(!starts_with("V"))
  
  svd_matrix <- sapply(svd_fields, as.numeric)
  svd_result = svd(svd_matrix)$u[, 1:3] # Scree plots show the first 3 columns almost always contain majority of variance.
  stock_df <- cbind(stock_df, svd_result)
}

# APPLY SVD LOCALLY
ose_reduced <- ose_stocks %>% # group the dataframe by ticker (2 columns: str_ticker, df_stock)
  group_by(Ticker) %>% 
  nest()
ose_reduced$data <- lapply(ose_reduced$data, run_svd)
# unnest and rename columns
ose_reduced <- ose_reduced %>% 
  unnest(c(Ticker, data))
colnames(ose_reduced)[4] <- "SVD1"
colnames(ose_reduced)[5] <- "SVD2"
colnames(ose_reduced)[6] <- "SVD3"

# Verify local SVD and Spark SVD are equal
#all_equal(ose_reduced_local, ose_reduced)

# Import sentiment analysis file and join to reduced dataframe
sentiment <- read.csv(file="Sentiment.csv", header=FALSE)
names(sentiment)[names(sentiment) == "V1"] <- "Ticker"
names(sentiment)[names(sentiment) == "V2"] <- "DateTime"
names(sentiment)[names(sentiment) == "V3"] <- "Sentiment"
sentiment[["DateTime"]] <- as.POSIXct(strptime(sentiment[["DateTime"]], format = "%Y-%m-%d %H:%M:%S", tz="Europe/Oslo"))

# Merge dataframes and set periods without a news score to 0
ose_reduced <- merge(x = ose_reduced, y = sentiment, by = c("Ticker", "DateTime"), all.x = TRUE)
ose_reduced[is.na(ose_reduced)] <- 0



run_auto.arima <- function(stock_df) {
  library(forecast)
  library(dplyr)
  fit <- auto.arima(stock_df$ClosePrice + stock_df$SVD1 + stock_df$SVD2 + stock_df$SVD3)
  return(fit)
}

# RUN auto.arima LOCALLY
arima_results <- ose_reduced %>% # group the dataframe by ticker (2 columns: str_ticker, df_stock)
  group_by(Ticker) %>% 
  nest()
arima_results <- lapply(arima_results$data, run_auto.arima) # apply run_svd function to each element

# unique stocks
unique(ose_reduced$Ticker)

# ADAPTED FROM:
# https://business-science.github.io/modeltime/articles/modeltime-spark.html
# FOR USE ON STOCK DATA WITH ARIMA(X) MODEL AND EXOGENOUS REGRESSORS ADDED
library(sparklyr)
library(tidymodels)
library(modeltime)
library(tidyverse)
library(timetk)
# initialize Spark connection
sc <- spark_connect(master = "local")
parallel_start(sc, .method = "spark")

ticker_list <- c("EQNR", "AKER", "SUBC", "BAKKA", "LSG", "SALME", "NHY", "YAR", "ELK")

# View data
ose_reduced %>%
  filter(Ticker %in% ticker_list) %>% 
#  filter(DateTime > "2021-09-01" & DateTime < "2021-11-30") %>% 
  select(Ticker, DateTime, ClosePrice) %>%
  set_names(c("ticker", "date", "close")) %>%
  group_by(ticker) %>%
  plot_time_series(date, close, .facet_ncol = 3, .interactive = FALSE)


# Build nested data table
nested_data_tbl <- ose_reduced %>%
  filter(Ticker %in% ticker_list) %>% 
#  filter(DateTime > "2021-09-01" & DateTime < "2021-11-30") %>% 
  set_names(c("ticker", "date", "close", "svd1", "svd2", "svd3", "sentiment")) %>%
  extend_timeseries(
    .id_var        = ticker,
    .date_var      = date,
    .length_future = 200
  ) %>%
  nest_timeseries(
    .id_var        = ticker,
    .length_future = 200
  ) %>%
  
  split_nested_timeseries(
    .length_test = 200
  )


# Build models
# XGBoost uses decision trees and gradient boosting
rec_xgb <- recipe(close ~ ., extract_nested_train_split(nested_data_tbl)) %>%
  step_timeseries_signature(date) %>%
  step_rm(date) %>%
  step_zv(all_predictors()) %>%
  step_dummy(all_nominal_predictors(), one_hot = TRUE)

wflw_xgb <- workflow() %>%
  add_model(boost_tree("regression") %>% 
              set_engine("xgboost")) %>%
  add_recipe(rec_xgb)

# PROPHET W/O regressors
rec_prophet <- recipe(close ~ date, extract_nested_train_split(nested_data_tbl)) 
wflw_prophet <- workflow() %>%
  add_model(
    prophet_reg("regression") %>% 
      set_engine("prophet")
  ) %>%
  add_recipe(rec_prophet)


# PROPHET
rec_prophet_xreg <- recipe(close ~ date + svd1 + svd2 + svd3 + sentiment, extract_nested_train_split(nested_data_tbl)) 
wflw_prophet_xreg <- workflow() %>%
  add_model(
    prophet_reg("regression") %>% 
    set_engine("prophet")
  ) %>%
  add_recipe(rec_prophet_xreg)

# AUTO ARIMA
rec_auto_arima <- recipe(close ~ date, extract_nested_train_split(nested_data_tbl)) 
wflw_auto_arima <- workflow() %>%
  add_model(
    arima_reg() %>% 
      set_engine(engine = "auto_arima")
  ) %>%
  add_recipe(rec_auto_arima)

# AUTO ARIMA(X)
rec_auto_arimax <- recipe(close ~ date + svd1 + svd2 + svd3 + sentiment, extract_nested_train_split(nested_data_tbl)) 
wflw_auto_arimax <- workflow() %>%
  add_model(
    arima_reg() %>% 
      set_engine(engine = "auto_arima")
  ) %>%
  add_recipe(rec_auto_arimax)

# Fit models in parallel
nested_modeltime_tbl <- nested_data_tbl %>%
  modeltime_nested_fit(
    wflw_xgb,
    wflw_prophet,
    wflw_prophet_xreg,
    wflw_auto_arima,
    wflw_auto_arimax,
    control = control_nested_fit(allow_par = TRUE, verbose = TRUE)
  )

nested_modeltime_tbl

# Accuracy
nested_modeltime_tbl %>%
  extract_nested_test_accuracy() %>%
  table_modeltime_accuracy(.interactive = F)

# Forecast
nested_modeltime_tbl %>%
  extract_nested_test_forecast() %>%
  group_by(ticker) %>%
  plot_modeltime_forecast(.facet_ncol = 3, .interactive = F)


# Shut down Spark
parallel_stop()
spark_disconnect_all()
