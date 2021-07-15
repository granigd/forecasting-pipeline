library(data.table)
library(lubridate)
library(ggplot2)
library(forecast)
library(BBmisc)
library(zoo)
library(tictoc)

source("classes/TimeSeries.R")
source("classes/ForecastEngine.R")
source("classes/PersistenceEngine.R")
source("classes/TimeSeriesForecast.R")
source("functions/read_from_file.R")
source("functions/read_from_database.R")


# Config
load_ts_from_db = FALSE
save_ts_to_db = FALSE
save_forecasts_to_db = TRUE
meta_file_location = "data/master_data_100.csv"
value_file_location = "data/movement_data_100.csv"

# acquire historical time series data
time_series_list = NULL
if (load_ts_from_db) {
  tic("loading historical values from db")
  time_series_list = read_ts_from_db(meta_file_location)
  toc()
} else {
  tic("loading historical values from file")
  time_series_list = read_ts_from_file(meta_file_location, value_file_location)
  toc()
}

# open connection to InfluxDB for writing
persistence_engine = PersistenceEngine$new()
# initialize the database - in case it's the first time
persistence_engine$init_forecast_db()

# persist the historical values 
if (save_ts_to_db) {
  tic("persisting historical values")
  for (time_series in time_series_list) {
    persistence_engine$save_ts(time_series)
  }
  toc()
}


# perform forecasts
forecast_engine = ForecastEngine$new()
tic("performing forecasts")
time_series_forecast_list = list()
for (time_series in time_series_list) {
  time_series_forecast = forecast_engine$forecast(time_series = time_series,
                                                  horizon = time_series$h,
                                                  error_method = "rmse")
  time_series_forecast_list = append(time_series_forecast_list, time_series_forecast)
}
toc()

# persist forecasts
tic("persisting forecasts")
for (time_series_forecast in time_series_forecast_list) {
  persistence_engine$save_forecast(time_series_forecast)
}
toc()


# load forecast
tic("loading forecasts from db")
time_series_forecast_list2 = read_ts_from_db(meta_file_location, ts_type = "TimeSeriesForecast")
toc()

