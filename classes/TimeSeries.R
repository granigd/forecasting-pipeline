library(R6)


TimeSeries <- R6Class("TimeSeries", list(
  id = NULL,
  valid_from = NULL,
  valid_to = NULL,
  freq = "day",
  h = NULL,
  values = NULL,
  initialize = function(id, valid_from, valid_to, freq = "day", h, values) {
    stopifnot(is.character(id), length(id) == 1)
    stopifnot(is.Date(valid_from))
    stopifnot(is.Date(valid_to))
    stopifnot(is.character(freq), is.element(freq, c('day', 'week', 'month', 'year')))
    stopifnot(is.numeric(h))
    stopifnot(is.data.table(values))
    
    
    self$id <- id
    self$valid_from <- valid_from
    self$valid_to <- valid_to
    self$freq <- freq
    self$h <- h
    self$values <- values
  },
  make_ts = function() {
    # TODO: calculate frequency based on $freq
    ts_values <- ts(movement_data_list[[4]]$quantity, start = decimal_date(min(movement_data_list[[4]]$date_time)), frequency = 365.25/7)
    return(ts_values)
  }
))

