source("classes/TimeSeries.R")
source("classes/PersistenceEngine.R")


read_ts_from_db <- function(meta_file_location, ts_type = "TimeSeries") {
  
  # read meta info file
  meta_info <- fread(meta_file_location)

  # count number of rows and prepare result set
  N = nrow(meta_info)
  time_series_list <- vector("list", N)
  
  # optain a connection to influxdb via PersistenceEngine
  connector = PersistenceEngine$new()
  
  # loop over each warehouse/product combination
  for (i in 1:N) { 
    
    warehouse_id = meta_info[i]$warehouse_id
    article_id = meta_info[i]$article_id
    n_steps = meta_info[i]$n_steps
    from = meta_info[i]$from
    to = meta_info[i]$to
    if (ts_type == "TimeSeries") {
      tso = connector$load_ts(warehouse_id, article_id, n_steps, from, to)
    }
    else {
      tso = connector$load_forecast(warehouse_id, article_id, n_steps, from, to)
    }
    time_series_list[[i]] = tso
    
  }
  
  return(time_series_list)
}

