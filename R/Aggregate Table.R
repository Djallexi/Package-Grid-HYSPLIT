library(data.table)

aggregate_table <- function(dt, 
                               by_uid    = TRUE, 
                               by_month  = TRUE,
                               lat_col   = "lat",
                               lon_col   = "lon",
                               uid_col   = "uID",
                               month_col = "yearmonth",
                               value_col = "hyads") {
  
  group_keys <- c(
    lat_col, lon_col,
    if (by_uid)   uid_col,
    if (by_month) month_col
  )
  
  dt[, .(value = sum(get(value_col), na.rm = TRUE)), by = group_keys] |>
    setnames("value", value_col)
}