combine_monthly_links <- function(
    month_YYYYMMs,
    link.to  = "grids",
    filename = NULL
) {
  if (link.to != "grids") stop("Only link.to = 'grids' is supported.")
  
  names.map <- c()
  
  for (ym in month_YYYYMMs) {
    year.h  <- substr(ym, 1, 4)
    month.m <- as.integer(substr(ym, 5, 6))
    month.h <- formatC(month.m, width = 2, format = "d", flag = "0")
    
    pattern     <- paste0("gridlinks.*", year.h, "-", month.h, ".*\\.fst$")
    files.month <- list.files(path = links_dir, pattern = pattern, full.names = TRUE)
    
    if (length(files.month) == 0) {
      print(paste("No data files for month_YYYYMMs", ym))
      next
    }
    
    print(paste("Reading and merging month", month.h, "in year", year.h))
    
    unitnames <- basename(files.month)
    unitnames <- sub("^gridlinks_", "", unitnames)
    unitnames <- sub(paste0("_", year.h, "-", month.h, ".*fst$"), "", unitnames)
    names(files.month) <- unitnames
    
    data.h      <- lapply(seq_along(files.month), read_gridlinks_subfun, files.month)
    MergedDT    <- rbindlist(data.h)
    Merged_cast <- dcast(MergedDT, x + y ~ ID, fun.aggregate = sum, value.var = "N")
    
    name.map  <- paste0("MAP", month.m, ".", year.h)
    names.map <- append(names.map, name.map)
    assign(name.map, Merged_cast)
    rm(MergedDT, Merged_cast)
  }
  
  if (length(names.map) == 0) {
    warning("Aucune donnée trouvée — retourne NULL.")
    return(NULL)
  }
  
  if (is.null(filename)) filename <- paste0("hyads_unwgted_", link.to, ".RData")
  rda.filename <- file.path(rdata_dir, filename)
  save(list = names.map, file = rda.filename)
  print(paste("Monthly RData file written to", rda.filename))
  return(mget(names.map))
}
