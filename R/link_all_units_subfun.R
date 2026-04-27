#' @export disperser_link_grids
disperser_link_grids <- function(month_YYYYMM = NULL,
                                 start.date = NULL,
                                 end.date = NULL,
                                 unit,
                                 duration.run.hours = duration.run.hours,
                                 pbl.height,
                                 res.link. = 12000,
                                 overwrite = F,
                                 pbl. = TRUE,
                                 crop.usa = FALSE,
                                 return.linked.data.) {
  
  unitID <- unit$ID
  
  if ((is.null(start.date) | is.null(end.date)) & is.null(month_YYYYMM))
    stop("Define either a start.date and an end.date OR a month_YYYYMM")
  if (dim(unit)[1] > 1)
    stop("Please supply a single unit (not multiple)")
  
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
  
  #' @export link_to
  link_to <- function(d,
                      link.to = 'grids',
                      p4string,
                      zc = NULL,
                      cw = NULL,
                      county.sp = NULL,
                      rasterin = NULL,
                      res.link. = 12000,
                      pbl. = TRUE,
                      crop.usa = FALSE) {
    
    # 1. Même traitement des points
    xy <- d[, .(lon, lat)]
    pts_sf <- sf::st_as_sf(d, coords = c("lon", "lat"), crs = 4326)
    pts_sf <- sf::st_transform(pts_sf, crs = p4string)
    
    
    
    # 2. Même calcul de l'étendue
    coords <- sf::st_coordinates(pts_sf)
    e_xmin <- floor(min(coords[,1]) / res.link.) * res.link.
    e_ymin <- floor(min(coords[,2]) / res.link.) * res.link.
    e_xmax <- ceiling(max(coords[,1]) / res.link.) * res.link.
    e_ymax <- ceiling(max(coords[,2]) / res.link.) * res.link.
    
    # 3. Création du raster avec centres alignés
    coords <- sf::st_coordinates(pts_sf)
    xmin_pts <- min(coords[,1])
    xmax_pts <- max(coords[,1])
    ymin_pts <- min(coords[,2])
    ymax_pts <- max(coords[,2])
    
    # Calcul du premier et dernier centre souhaités (multiples de res.link.)
    first_center_x <- floor((xmin_pts + res.link./2) / res.link.) * res.link.
    last_center_x  <- ceiling((xmax_pts - res.link./2) / res.link.) * res.link.
    first_center_y <- floor((ymin_pts + res.link./2) / res.link.) * res.link.
    last_center_y  <- ceiling((ymax_pts - res.link./2) / res.link.) * res.link.
    
    # Nombre de cellules
    ncols <- (last_center_x - first_center_x) / res.link. + 1
    nrows <- (last_center_y - first_center_y) / res.link. + 1
    
    # Limites du raster (coins des cellules)
    xmin_raster <- first_center_x - res.link./2
    xmax_raster <- xmin_raster + ncols * res.link.
    ymin_raster <- first_center_y - res.link./2
    ymax_raster <- ymin_raster + nrows * res.link.
    
    # Création du raster
    r <- terra::rast(
      xmin = xmin_raster, xmax = xmax_raster,
      ymin = ymin_raster, ymax = ymax_raster,
      resolution = res.link.,
      crs = p4string
    )
    
    # 4. Comptage des cellules (identique)
    cells <- terra::cellFromXY(r, coords)
    tab <- table(cells)
    
    # 5. Traitement PBL - CORRECTION ICI
    if (pbl.) {
      if (!is.null(rasterin)) {
        pbl_layer <- subset_nc_date(hpbl_brick = rasterin, vardate = d$Pdate[1])
        
        # Convertir en terra si nécessaire
        if (!inherits(pbl_layer, "SpatRaster")) {
          pbl_layer <- terra::rast(pbl_layer)
        }
        
        # Reprojection
        pbl_layer_proj <- terra::project(pbl_layer, r)
        
        # Extraire valeurs PBL pour les cellules - SANS FILTRE
        pbl_values <- terra::extract(pbl_layer_proj, 
                                     terra::xyFromCell(r, as.numeric(names(tab))))[, 1]
        
        # Calcul direct comme l'original (même si NA ou 0)
        vals <- as.numeric(tab) / pbl_values
        
        # Assigner au raster
        r[as.numeric(names(tab))] <- vals
      } else {
        # Si rasterin est NULL, utiliser sans PBL
        warning("rasterin est NULL, utilisation sans PBL")
        r[as.numeric(names(tab))] <- as.numeric(tab)
      }
    } else {
      r[as.numeric(names(tab))] <- as.numeric(tab)
    }
    
    # 6. Trim et crop - MÊME LOGIQUE
    # D'abord trim
    r_trimmed <- terra::trim(r)
    
    # Puis crop à l'étendue originale
    r2 <- terra::crop(r_trimmed, terra::ext(e_xmin, e_xmax, e_ymin, e_ymax))
    
    # 7. Crop USA si demandé
    if (crop.usa) {
      usa <- rnaturalearth::ne_countries(
        scale = 110, type = "countries",
        country = "United States of America",
        returnclass = "sf"
      )
      
      # Prendre le 6ème polygone comme l'original
      usa.sub <- sf::st_cast(usa, "POLYGON")[6,]
      usa.sub <- sf::st_transform(usa.sub, crs = p4string)
      
      r2 <- terra::crop(r2, terra::vect(usa.sub))
    }
    
    # 8. Retour selon link.to
    if (link.to == 'grids') {
      xyz <- as.data.table(terra::as.data.frame(r2, xy = TRUE, na.rm = TRUE))
      names(xyz)[3] <- 'N'
      return(xyz)
    }
    
    # Convertir en polygones
    r3_poly <- terra::as.polygons(r2, na.rm = TRUE)
    r3_sf <- sf::st_as_sf(r3_poly)
    
    if (link.to == 'counties') {
      print('Linking counties!')
      
      # Convertir county.sp en sf
      county_sf <- sf::st_as_sf(county.sp)
      county_sf <- sf::st_transform(county_sf, crs = p4string)
      
      # Intersection spatiale - reproduire over(..., fn = mean)
      intersected <- sf::st_intersection(county_sf, r3_sf)
      
      # Calculer la moyenne par comté
      county_stats <- intersected %>%
        sf::st_drop_geometry() %>%
        group_by(statefp, countyfp, state_name, name, geoid) %>%
        summarise(N = mean(get(names(r3_sf)[1]), na.rm = TRUE), .groups = "drop") %>%
        as.data.table()
      
      # Ajouter les comtés sans intersection avec NA
      all_counties <- county_sf %>%
        sf::st_drop_geometry() %>%
        select(statefp, countyfp, state_name, name, geoid) %>%
        as.data.table()
      
      result <- merge(all_counties, county_stats, 
                      by = c("statefp", "countyfp", "state_name", "name", "geoid"),
                      all.x = TRUE)
      
      return(result)
    }
    
    if (link.to == 'zips') {
      # Convertir zc en sf
      zc_sf <- sf::st_as_sf(zc)
      zc_sf <- sf::st_transform(zc_sf, crs = p4string)
      
      # Crop à l'étendue
      bbox <- sf::st_bbox(c(xmin = e_xmin, xmax = e_xmax, 
                            ymin = e_ymin, ymax = e_ymax), crs = p4string)
      zc_trim <- sf::st_crop(zc_sf, bbox)
      
      # Intersection spatiale
      intersected <- sf::st_intersection(zc_trim, r3_sf)
      
      # Calculer la moyenne par ZCTA
      zip_stats <- intersected %>%
        sf::st_drop_geometry() %>%
        group_by(ZCTA5CE10) %>%
        summarise(N = mean(get(names(r3_sf)[1]), na.rm = TRUE), .groups = "drop") %>%
        as.data.table()
      
      setnames(zip_stats, "ZCTA5CE10", "ZCTA")
      
      # Fusionner avec crosswalk
      cw$ZCTA <- formatC(cw$ZCTA, width = 5, format = "d", flag = "0")
      M <- merge(zip_stats, cw, by = "ZCTA", all = FALSE, allow.cartesian = TRUE)
      M[, ZIP := formatC(ZIP, width = 5, format = "d", flag = "0")]
      M$ZIP <- as(M$ZIP, 'character')
      M <- na.omit(M)
      
      return(M)
    }
  }
  
  trim_zero <- function (Min) 
  {
    M <- copy(Min)
    p_zero_df <- M[height == 0, ]
    particles <- unique(p_zero_df$particle_no)
    for (p in particles) {
      h_zero <- p_zero_df[particle_no == p, hour]
      M[particle_no == p & hour >= h_zero, ] <- NA
    }
    M <- na.omit(M)
    return(M)
  }
  
  
  ## create start.date and end.date if month_YYYYMM is provided
  if (is.null(start.date) | is.null(end.date)) {
    start.date <- as.Date(paste(substr(month_YYYYMM, 1, 4),
                                substr(month_YYYYMM, 5, 6),
                                '01', sep = '-'))
    end.date <- seq(start.date, by = paste(1, "months"), length = 2)[2] - 1
  }
  
  if (is.null(month_YYYYMM))
    month_YYYYMM <- paste(start.date, end.date, sep = '_')
  
  month_YYYYMM <- as(month_YYYYMM, 'character')
  
  ## name the eventual output file
  output_file <- file.path(links_dir,
                           paste0("gridlinks_",
                                  unit$ID, "_",
                                  start.date, "_",
                                  end.date, ".fst"))
  
  ## Run the grid linkages
  if (!file.exists(output_file) | overwrite == T) {
    
    ## identify dates for hysplit averages and dates for files to read in
    vec_dates <- as(seq.Date(as.Date(start.date),
                             as.Date(end.date),
                             by = '1 day'), 'character')
    
    vec_filedates <- seq.Date(from = as.Date(start.date) - ceiling(duration.run.hours / 24),
                              to   = as.Date(end.date),
                              by   = "1 day")
    
    ## list the files
    pattern.file <- paste0('_',
                           gsub('[*]', '[*]', unit$ID),
                           '_(',
                           paste(vec_filedates, collapse = '|'),
                           ').*\\.fst$')
    
    hysp_dir.path <- file.path(hysp_dir,
                               unique(paste(year(vec_filedates),
                                            formatC(month(vec_filedates), width = 2, flag = '0'),
                                            sep = '/')))
    
    files.read <- list.files(path      = hysp_dir.path,
                             pattern   = pattern.file,
                             recursive = F,
                             full.names = T)
    
    ## read in the files
    l <- lapply(files.read, read.fst, as.data.table = TRUE)
    
    ## Combine all parcels into single data table
    d <- rbindlist(l)
    if (length(d) == 0)
      return(paste("No files available to link in", month_YYYYMM))
    print(paste(Sys.time(), "Files read and combined"))
    
    ## Trim dates & first hour
    d <- d[as(Pdate, 'character') %in% vec_dates & hour > 1, ]
    
    ## Trim PBL's
    if (pbl.) {
      d_xmin <- min(d$lon)
      e_xmin <- extent(pbl.height)[1]
      if (d_xmin < e_xmin - 5)
        pbl.height <- rotate(pbl.height)
      d_trim <- trim_pbl(d, rasterin = pbl.height)
      print(paste(Sys.time(), "PBLs trimmed"))
    } else {
      d_trim <- d
    }
    
    ## On projette en LAEA centrée sur l'Inde (mètres) pour le calcul du raster.
    ## Raison : en WGS84 lat/lon les cellules tombent sur une grille régulière
    ## en degrés → les gaps entre trajectoires créent un quadrillage visible.
    ## En projection métrique les centres de cellules sont irréguliers en WGS84
    ## une fois reprojetés → dispersion naturelle, pas de quadrillage.
    p4s_wgs84 <- "+proj=longlat +datum=WGS84 +no_defs"
    
    disp_df_link <- link_to(d         = d_trim,
                            link.to   = 'grids',
                            p4string  = p4s_wgs84,
                            rasterin  = pbl.height,
                            res.link. = 0.1,
                            pbl.      = pbl.,
                            crop.usa  = crop.usa)
    
    ## ── Renommage x,y → lon,lat ──────────────────────────────────────────────
    out <- disp_df_link
    #setnames(out, c("x", "y"), c("lon", "lat"))
    
    print(paste(Sys.time(), "Grids linked in WGS84 0.3°"))
    
    out$month <- as(month_YYYYMM, 'character')
    out$ID    <- unitID
    
    if (nrow(out) != 0) {
      write.fst(out, output_file)
      print(paste(Sys.time(), "Linked grids saved to", output_file))
    }
    
  } else {
    print(paste("File", output_file, "already exists! Use overwrite = TRUE to overwrite"))
    if (return.linked.data.)
      out <- read.fst(output_file, as.data.table = TRUE)
  }
  
  if (!return.linked.data.)
    out <- data.table(lon = numeric(), lat = numeric(), N = numeric())
  
  out$month <- as(month_YYYYMM, 'character')
  out$ID    <- unitID
  suppressWarnings(out[, V1 := NULL])
  return(out)
}
