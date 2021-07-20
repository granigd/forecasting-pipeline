library(R6)
library(forecast)
source("classes/TimeSeriesForecast.R")

ForecastEngine <- R6Class("ForecastEngine", list(
  supported_functions = c(rwf, snaive, ses),
  supported_functions_display_names = c("Naive", "Seasonal Naive", "Exponential Smoothing"),
  supported_error_methods = c("rmse"),
  best_method = NULL,
  report = NULL,
  initialize = function() {
  },
  calculate_error = function(rsiduals, error_method) {
    if (error_method == "rmse") {
      error = sqrt(mean(rsiduals^2, na.rm=TRUE))
    } else {
      error = sqrt(mean(rsiduals^2, na.rm=TRUE))
    }
    return(error)
  },
  perform_crossv = function(ts_obj, horizon) {
    # perform cross validation for each method and calculate:
    # 1. The smallest Error
    # 2. The best method
    methods = c()
    scores = c()
    best_method = NULL
    smallest_error = 1/0
    method_counter = 1
    for (method in self$supported_functions) {
      res = tsCV(ts_obj, method, h=horizon)
      error = self$calculate_error(res, "rmse")
      
      if (error < smallest_error) {
        smallest_error = error 
        best_method = method
      }
      
      methods = append(methods, self$supported_functions_display_names[method_counter])
      scores = append(scores, error)
      method_counter = method_counter + 1
    }
    self$best_method = best_method
    self$report = data.frame(methods, scores)
  },
  forecast = function(time_series, horizon, error_method = "rmse") {
    cat("Performing forecast for time_series of warehouse: ", 
        time_series$warehouse_id, " and article: ", time_series$article_id, "\n")
    
    # create a ts object from the TimeSeries object
    ts_obj = ts(time_series$values$quantity, 
                start = decimal_date(min(time_series$values$time)), 
                frequency = 365.25/7)
   
    # perform cross validation and determine best method
    self$perform_crossv(ts_obj, horizon)
    # perform the forecast with the best method
    ts_forecast = self$best_method(ts_obj, h=horizon)
    
    # Logging Output
    #cat("Best Method: ", ts_forecast$method, "\n")
    #cat("Error Value: ", smallest_error, "\n")
 
    # calculate the forecasted timestamps 
    days = c()
    last_date = tail(time_series$values$time, 1)
    for (x in 1:horizon) {
      days = append(days, last_date + 7)
      last_date = last_date + 7
    }
    
    # extract the values from the forecast and make a data frame
    if (!is.null(ts_forecast$model$future)) {
      df = data.frame(best_forecast=as.numeric(head(ts_forecast$model$future, 1)),
                           time=days,
                           n=horizon,
                           best_method=ts_forecast$method)
             
    } else {
      
      df = data.frame(best_forecast=as.numeric(tail(ts_forecast$model$states, 1)),
                           time=days,
                           n=horizon,
                           best_method=ts_forecast$method)
               
    }
    colnames(df) <- c('best_forecast','time', 'n', 'best_method')
    # in case the forecast produced floats -> round them to next integer
    nums <- vapply(df, is.numeric, FUN.VALUE = logical(1))
    df[,nums] <- round(df[,nums], digits = 0)
    
    # create a forecast object from the ts object
    forecast_object <- TimeSeriesForecast$new(warehouse_id = time_series$warehouse_id,
                                              article_id = time_series$article_id,
                                              historical_values = time_series$values,
                                              values = df,
                                              horizon = horizon,
                                              forecast_from = time_series$valid_to,
                                              forecast_to = time_series$valid_to,
                                              freq = time_series$freq,
                                              forecast_method = self$best_method,
                                              evaluation_report = self$report)
    
    return(forecast_object)
  }
))

