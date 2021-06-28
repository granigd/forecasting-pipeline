library(data.table)
library(lubridate)
library(ggplot2)


# read meta info file
meta_info <- fread("data/master_data.csv")
# make a combined unique id from warehouse/article id and 
meta_info[, id:=paste(warehouse_id, article_id, sep=".")]
# set it as key for faster lookup-time
setkey(meta_info, id)


# load historic values
movement_data <- fread("data/movement_data.csv")
# re-name date_time to time
names(movement_data)[names(movement_data)=="date_time"] <- "time"


# split up data table into multiple data tables depending on identifier
movement_data_list = split(movement_data, 
                           with(movement_data, 
                                interaction(warehouse_id, article_id)), 
                           drop = TRUE)

# loop over each warehouse/product combination
for (i in 1:length(movement_data_list)) { 
  
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
  
}


# save to class
combined_id = names(movement_data_list[1])
meta_info_entry = meta_info[.(combined_id)]
n_steps = meta_info_entry$n_steps
from = meta_info_entry$from
to = meta_info_entry$to

historic_values_1 <- TimeSeries$new(id = combined_id, 
                                        valid_from = from, 
                                        valid_to = to,
                                        h = n_steps, 
                                        values = movement_data_list[[1]])

historic_values_1$make_ts()


# clear up environment
rm(list = ls())

