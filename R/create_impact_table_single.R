#' @export create_impact_table_single
create_impact_table_single <- function(data.linked,
                                       data.units,
                                       link.to = 'zips',
                                       zcta.dataset = NULL,
                                       counties. = NULL,
                                       map.month,
                                       map.unitID,
                                       metric = 'N') {
  
  # --- Pas de vérifications d'arguments pour coller à l'ancien code ---
  year.use <- as.integer(substr(map.month, 1, 4))
  
  datareduced <- data.linked[month == map.month & ID == map.unitID]
  dataunits   <- data.units[ID == map.unitID & year == year.use]
  
  if (link.to == 'zips') {
    # Fusion identique à l'ancien code
    dataset_sf <- data.table(
      dataunits,
      merge(zcta.dataset, datareduced, by = 'ZIP', all.y = TRUE)
    )
    setnames(dataset_sf, metric, 'metric')
    myVector <- c(
      "ID", "month", "Latitude", "Longitude", "metric",
      "ZIP", "ZCTA", "CITY", "STATE", "TOTALESTIMATE", "MARGINOFERROR",
      "geometry"
    )
    
  } else if (link.to == 'counties') {
    # Sélection des colonnes counties identique à l'ancien code
    counties_sub <- counties.[, c("statefp", "countyfp", "state_name",
                                  "name", "geoid", "geometry")]
    dataset_sf <- data.table(
      dataunits,
      merge(counties_sub, datareduced,
            by = c("statefp", "countyfp", "state_name", "name", "geoid"),
            all.y = TRUE)
    )
    setnames(dataset_sf, metric, 'metric')
    myVector <- c(
      "ID", "month", "Latitude", "Longitude", "metric",
      "statefp", "countyfp", "state_name", "name",
      "geometry"   # PAS de "geoid"
    )
    
  } else if (link.to == 'grids') {
    # --- Reproduction stricte de l'ancien code avec raster/sp ---
    if (!requireNamespace("raster", quietly = TRUE))
      stop("Le package 'raster' est requis pour link.to = 'grids'")
    if (!requireNamespace("sp", quietly = TRUE))
      stop("Le package 'sp' est requis pour link.to = 'grids'")
    
    # rasterFromXYZ utilise les deux premières colonnes comme coordonnées
    # et la troisième comme valeur (supposée être 'metric')
    dataset_r <- suppressWarnings(raster::rasterFromXYZ(datareduced))
    dataset_sp <- as(dataset_r, "SpatialPolygonsDataFrame")
    dataset_sf <- sf::st_as_sf(dataset_sp)
    suppressWarnings(
      sf::st_crs(dataset_sf$geometry) <-
        "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m"
    )
    setnames(dataset_sf, metric, 'metric')
    # Ajout des colonnes ID et comb (exactement comme dans l'ancien code)
    dataset_sf$ID   <- datareduced$ID
    dataset_sf$comb <- datareduced$comb
    myVector <- names(dataset_sf)   # toutes les colonnes
  } else {
    stop("link.to doit être 'zips', 'counties' ou 'grids'")
  }
  
  # Conversion finale en data.table (identique à l'ancien code)
  out <- data.table(dataset_sf[, myVector, with = FALSE])
  return(out)
}