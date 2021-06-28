library(dplyr)
library(R6)
library(influxdbr)
library(data.table)


InfluxConnector <- R6Class("InfluxConnector", list(
  host = NULL,
  port = NULL,
  username = NULL,
  password = NULL,
  conn = NULL,
  db_name = NULL,
  3 = function(host = "localhost", port = 8086, 
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
  persist_df = function(data) {
    print("Writing to database.")
    influx_write(data,
                 con = self$conn, 
                 db = self$db_name,
                 rp = "tenyears",
                 precision = "h",
                 time_col = "time", tag_cols = c("warehouse_id", "article_id"),
                 measurement = "article_demand_forecasts")
   # print(show_measurements(con = self$conn, db = "article_demand_forecasts"))
  },
  read_df = function(warehouse_id, article_id) {

    result <- influx_select(con = self$conn, 
                  db = self$db_name, 
                  field_keys = c("quantity"), 
                  measurement = "article_demand_forecasts",
                  where = paste("warehouse_id = '", warehouse_id, "' and article_id = '", article_id, "'", sep = ""),
                  order_desc = FALSE, 
                  return_xts = FALSE, 
                  simplifyList = TRUE)
    return(result)
  }
))

