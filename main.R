library(data.table)
library(lubridate)
library(ggplot2)
library(forecast)
library(BBmisc)
library(zoo)

source("classes/TimeSeries.R")
source("classes/InfluxConnector.R")
source("functions/read_from_file.R")
source("functions/read_from_database.R")


# Config
load_from_db = FALSE
meta_file_location = "data/master_data.csv"
value_file_location = "data/movement_data.csv"

# acquire input data
time_series_list = NULL
if (load_from_db) {
  time_series_list = read_ts_from_db(meta_file_location, value_file_location)
} else {
  time_series_list = read_ts_from_file(meta_file_location, value_file_location)
}

# open connection to Influxdb for writing
connector = InfluxConnector$new()
connector$init_forecast_db()

# perform forecasting and evaluation
N = length(time_series_list)
for (i in 1:N) { 
  
  ts_obj = ts(time_series_list[[i]]$values$quantity, 
              start = decimal_date(min(time_series_list[[i]]$values$time)), 
              frequency = 365.25/7)
  h = time_series_list[[i]]$h
  
  # perform cross validation of and calculate the RMSE
  
  # NAIVE METHOD
  res = tsCV(ts_obj, rwf, h=h)
  rmse_naive = sqrt(mean(res^2, na.rm=TRUE))
  
  best_method = rwf
  best_rmse = rmse_naive
  
  # SEASONAL NAIVE METHOD
  res = tsCV(ts_obj, snaive, h=h)
  rmse_snaive = sqrt(mean(res^2, na.rm=TRUE))
  
  if (rmse_snaive < best_rmse) {
    best_method = snaive
    best_rmse = rmse_snaive
  }
  
  # EXP SMOOTH METHOD
  res = tsCV(ts_obj, ses, h=h)
  rmse_ses = sqrt(mean(res^2, na.rm=TRUE))
  
  if (rmse_ses < best_rmse) {
    best_method = ses
    best_rmse = rmse_ses
  }


  ts_forecast = best_method(ts_obj, h=h)
  
  cat("Best Method: ", ts_forecast$method, "\n")
  cat("RMSE Value: ", best_rmse, "\n")
  
  if (!is.null(ts_forecast$model$future)) {
    df = head(data.frame(best_forecast=as.matrix(ts_forecast$model$future),
                         quantity=as.matrix(ts_forecast$model$future),
                         time=as.Date(as.yearmon(time(ts_forecast$model$future))),
                         n=h,
                         best_method=ts_forecast$method), 
              n = h)
  } else {
 
    df = tail(data.frame(best_forecast=as.matrix(ts_forecast$model$states),
                         quantity=as.matrix(ts_forecast$model$states),
                         time=as.Date(as.yearmon(time(ts_forecast$model$states))),
                         n=h,
                         best_method=ts_forecast$method), 
              n = h)
  }
  colnames(df) <- c('best_forecast','quantity','time', 'n', 'best_method')
  
  
  time_series_list[[i]]$values = df
  #df = time_series_list[[i]]$values
  df$warehouse_id = time_series_list[[i]]$warehouse_id
  df$article_id = time_series_list[[i]]$article_id
  df$time = as.POSIXct(df$time, origin="1970-01-01")
  print("Persisting")
  connector$persist_df(df)
  print("End")
}

# clear up environment
#rm(list = ls())

