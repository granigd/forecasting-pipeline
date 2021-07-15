library(dplyr)
library(R6)
library(influxdbr)
library(data.table)

PersistenceEngine <- R6Class("PersistenceEngine", list(
  host = NULL,
  port = NULL,
  username = NULL,
  password = NULL,
  conn = NULL,
  db_name = NULL,
  initialize = function(host = "localhost", port = 8086, 
                        username = NULL, password = NULL, db_name = "forecast_db") {
    self$host <- host
    self$port <- port
    self$username <- username
    self$password <- password
    self$db_name <- db_name
    self$conn <- influx_connection(host = host, port = port, 
                                          user = username, pass = password)
    
  },
  init_forecast_db = function() {
    # creates and sets up the database for our time series data
    existing_db = show_databases(con = self$conn) %>% filter(name == self$db_name)
    if (nrow(existing_db) != 0) {
      cat("Database", self$db_name, "already exists!")
      print("If you wish to clear the database, you have to issue a delete command first!")
      return()
    }
    cat("Creating database", self$db_name)
    create_database(con = self$conn, db = self$db_name)
    create_retention_policy(con = self$conn, db = self$db_name, rp_name = "tenyears", duration = "530w", replication = 1, default = TRUE)
  },
  delete_forecast_db = function() {
    # creates and sets up the database for our time series data
    existing_db = show_databases(con = self$conn) %>% filter(name == self$db_name)
    if (nrow(existing_db) == 0) {
      cat("Database", self$db_name, "does not exists!")
      return()
    }
    cat("Deleting database", self$db_name)
    drop_database(con = self$conn, db = self$db_name)
  },
  save_ts = function(time_series) {
    cat("Writing time series for", 
        time_series$warehouse_id, " ", 
        time_series$article_id ," to database. \n")
    
    values = time_series$values
    values$warehouse_id = time_series$warehouse_id
    values$article_id = time_series$article_id
    values$time = as.POSIXct(values$time, origin="1970-01-01")
    
    influx_write(values,
                 con = self$conn, 
                 db = self$db_name,
                 rp = "tenyears",
                 precision = "h",
                 time_col = "time", tag_cols = c("warehouse_id", "article_id"),
                 measurement = "article_demand_forecasts")
   # print(show_measurements(con = self$conn, db = "forecast_db"))
  },
  load_ts = function(warehouse_id, article_id, n_steps, from, to) {
    combined_id = paste(warehouse_id, article_id, sep=".")
    result <- influx_select(con = self$conn, 
                            db = self$db_name, 
                            field_keys = c("quantity"), 
                            measurement = "article_demand_forecasts",
                            where = paste("warehouse_id = '", warehouse_id, "' and article_id = '", article_id, "'", sep = ""),
                            order_desc = FALSE, 
                            return_xts = FALSE, 
                            simplifyList = TRUE)[[1]][,c('time','quantity')]
    
    tso <- TimeSeries$new(id = combined_id, 
                          valid_from = from, 
                          valid_to = to,
                          h = n_steps, 
                          values = result)
    return(tso)
  },
  save_forecast = function(time_series) {
    
    cat("Writing forecast for", 
        time_series$warehouse_id, " ", 
        time_series$article_id ," to database. \n")
    
    values = time_series$values
    values$warehouse_id = time_series$warehouse_id
    values$article_id = time_series$article_id
    values$time = as.POSIXct(values$time, origin="1970-01-01")
    
    influx_write(values,
                 con = self$conn, 
                 db = self$db_name,
                 rp = "tenyears",
                 precision = "h",
                 time_col = "time", tag_cols = c("warehouse_id", "article_id"),
                 measurement = "article_demand_forecasts")
  },
  load_forecast = function(warehouse_id, article_id, n_steps, from, to) {
    original_tso = self$load_ts(warehouse_id, article_id, n_steps, from, to)
    print(original_tso)
    result <- influx_select(con = self$conn, 
                            db = self$db_name, 
                            field_keys = 'best_forecast, n, best_method', 
                            measurement = "article_demand_forecasts",
                            where = paste("warehouse_id = '", warehouse_id, "' and article_id = '", article_id, "'",  " and best_forecast != -1", sep = ""),
                            order_desc = FALSE, 
                            return_xts = FALSE, 
                            simplifyList = TRUE)[[1]][,c('time','best_forecast', 'n', 'best_method')]
    result
    
    method = tail(result$best_method, 1)
    
    forecast_object <- TimeSeriesForecast$new(warehouse_id = warehouse_id,
                                              article_id = article_id,
                                              historical_values = time_series$values,
                                              values = result,
                                              horizon = n_steps,
                                              forecast_from = from,
                                              forecast_to = to,
                                              freq = time_series$freq,
                                              forecast_method = method,
                                              evaluation_report = NULL)
  }
))

