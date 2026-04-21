define_inputs <- function(units, startday, endday,
                          start.hours = c(0, 6, 12, 18),
                          duration    = 240) {
  startday.date <- as.Date(startday)
  endday.date   <- as.Date(endday)

  out <- data.table::data.table(
    expand.grid(
      ID                   = unique(units$ID),
      year                 = unique(units$year),
      start_hour           = start.hours,
      start_day            = seq.Date(from = startday.date, to = endday.date, by = "1 day"),
      duration_emiss_hours = 1,
      duration_run_hours   = duration,
      stringsAsFactors     = FALSE
    )
  )
  out[, start_day := as.Date(start_day, origin = "1970-01-01")]
  out <- out[year == year(start_day)]
  out <- unique(merge(out, units, by = c("ID", "year")))
  return(out)
}
