source("classes/TimeSeries.R")

read_ts_from_file <- function(meta_file_location, value_file_location) {
 
   # read meta info file
  meta_info <- fread(meta_file_location)
  # make a combined unique id from warehouse/article id and 
  meta_info[, id:=paste(warehouse_id, article_id, sep=".")]
  # set it as key for faster lookup-time
  setkey(meta_info, id)
  
  
  # load historic values
  movement_data <- fread(value_file_location)
  # re-name date_time to time
  names(movement_data)[names(movement_data)=="date_time"] <- "time"
  
  
  # split up data table into multiple data tables depending on identifier
  movement_data_list = split(movement_data, 
                             with(movement_data, 
                                  interaction(warehouse_id, article_id)), 
                             drop = TRUE)
  
  N = length(movement_data_list)
  time_series_list <- vector("list", N)
  
  # loop over each warehouse/product combination
  for (i in 1:N) { 
    
    # remove redundant information 
    movement_data_list[[i]][ ,c(1,2)] <- list(NULL)
    
    # get the id of the current time series
    combined_id = names(movement_data_list[i])
    
    # lookup associated meta info
    meta_info_entry = meta_info[.(combined_id)]
    n_steps = meta_info_entry$n_steps
    
    # create 'empty' time series which has no gaps in the time frame
    DT = data.table(
      time = seq(from = meta_info_entry$from, to = meta_info_entry$to, by=1),
      quantity = 0
    )
    # merge empty series with actual data
    movement_data_list[[i]] = rbindlist(list(movement_data_list[[i]], DT))
    
    # order series by time asc
    setorder(movement_data_list[[i]], time)
    
    # if same times_tamp is present multiple times in the series -> sum them up
    movement_data_list[[i]] = movement_data_list[[i]][,list(quantity=sum(quantity)),by=time]
    
    # aggregate by week (monday)
    movement_data_list[[i]] = movement_data_list[[i]][, .(quantity = sum(quantity)),
                                                      by = .(time = floor_date(time, "week", 1))]
    
    
    # save to class
    meta_info_entry = meta_info[.(combined_id)]
    n_steps = meta_info_entry$n_steps
    from = meta_info_entry$from
    to = meta_info_entry$to
    tso <- TimeSeries$new(id = combined_id, 
                                        valid_from = from, 
                                        valid_to = to,
                                        h = n_steps, 
                                        values = movement_data_list[[i]])
    time_series_list[[i]] = tso
    
  }
  
  return(time_series_list)
}