library(R6)
library(forecast)

TimeSeriesForecast <- R6Class("TimeSeriesForecast", list(
  warehouse_id = NULL,
  article_id = NULL,
  historical_values = NULL,
  values = NULL,
  horizon = NULL,
  forecast_from = NULL,
  forecast_to = NULL,
  forecast_method = NULL,
  evaluation_report = NULL,
  freq = NULL,
  initialize = function(warehouse_id, article_id, historical_values,
                        values, horizon, forecast_from, forecast_to,
                        forecast_method, evaluation_report, freq) {
    self$warehouse_id = warehouse_id
    self$article_id = article_id
    self$historical_values = historical_values
    self$values = values
    self$horizon = horizon
    self$forecast_from = forecast_from
    self$forecast_to = forecast_to
    self$forecast_method = forecast_method
    self$evaluation_report = evaluation_report
    self$freq = freq
  },
  plot_forecast = function() {
    plot(self$historical_values$time,
         self$historical_values$quantity,
         type="l", 
         main=cat(self$warehouse_id, " ", self$article_id, " horizon=", self$horizon),
         xlab="time",ylab="quantity")
    
    lines(self$values$time,
          self$values$quantity,
          lty=2,lwd=2,col="green")
  },
  plot_report = function() {
    plot(self$evaluation_report$method,
         self$evaluation_report$score,
         type="l", 
         main=cat("CV-Report for ", self$warehouse_id, " ", self$article_id, " horizon=", self$horizon),
         xlab="method",ylab="score")
  }
))

