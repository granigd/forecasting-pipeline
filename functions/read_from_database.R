source("classes/TimeSeries.R")
source("classes/InfluxConnector.R")


read_ts_from_db <- function(meta_file_location) {
  
  # read meta info file
  meta_info <- fread(meta_file_location)

  # count number of rows and prepare result set
  N = nrow(meta_info)
  time_series_list <- vector("list", N)
  
  # optain a connection to influxdb via InfluxConnector
  connector = InfluxConnector$new()
  
  x = meta_info[1]$warehouse_id
  
  # loop over each warehouse/product combination
  for (i in 1:N) { 
    
    warehouse_id = meta_info[i]$warehouse_id
    article_id = meta_info[i]$article_id
    combined_id = paste(warehouse_id, article_id, sep=".")
    n_steps = meta_info[i]$n_steps
    from = meta_info[i]$from
    to = meta_info[i]$to
    
    time_series_df = connector$read_df(warehouse_id,
                                       article_id)
    
    
    tso <- TimeSeries$new(id = combined_id, 
                          valid_from = from, 
                          valid_to = to,
                          h = n_steps, 
                          values = movement_data_list[[i]])
    
    time_series_list[[i]] = tso
    
  }
  
  return(time_series_list)
}