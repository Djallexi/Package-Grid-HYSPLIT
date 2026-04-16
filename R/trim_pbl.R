trim_pbl <- function (Min, rasterin) 
{
  Sys.setenv(TZ = "UTC")
  M <- copy(Min)
  M[, `:=`(ref, 1:nrow(M))]
  M[, `:=`(Pmonth, formatC(month(Pdate), width = 2, format = "d", 
                           flag = "0"))]
  M[, `:=`(Pyear, formatC(year(Pdate), width = 2, format = "d", 
                          flag = "0"))]
  my <- data.table(expand.grid(data.table(mo = unique(M[, 
                                                        Pmonth]), yr = unique(M[, Pyear]))))
  xy <- M[, .(lon, lat)]
  spdf <- SpatialPointsDataFrame(coords = xy, data = M, proj4string = CRS("+proj=longlat +datum=WGS84 +ellps=WGS84 +towgs84=0,0,0"))
  spdf$rastercell <- cellFromXY(rasterin, spdf)
  spdf.dt <- na.omit(data.table(spdf@data))
  for (m in 1:nrow(my)) {
    mon <- my[m, mo]
    yer <- my[m, yr]
    day <- paste(yer, mon, "01", sep = "-")
    pbl_layer <- subset_nc_date(hpbl_brick = rasterin, varname = "hpbl", 
                                vardate = day)
    spdf.dt[Pmonth %in% mon & Pyear %in% yer, `:=`(pbl, 
                                                   pbl_layer[spdf.dt[Pmonth %in% mon & Pyear %in% yer, 
                                                                     rastercell]])]
  }
  spdf.dt <- spdf.dt[height < pbl]
  return(M[spdf.dt$ref, .(lon, lat, height, Pdate, hour)])
}
