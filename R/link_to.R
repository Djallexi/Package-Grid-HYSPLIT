#' @export link_to
link_to <- function(d,
                    link.to   = 'grids',
                    p4string,
                    rasterin  = NULL,
                    res.link. = 12000,
                    pbl.      = TRUE,
                    crop.usa  = FALSE) {

  # 1. Transformer les points dans la projection cible
  pts_sf <- sf::st_as_sf(d, coords = c("lon", "lat"), crs = 4326)
  pts_sf <- sf::st_transform(pts_sf, crs = p4string)

  # 2. Calculer l'étendue
  coords <- sf::st_coordinates(pts_sf)
  e_xmin <- floor(min(coords[,1])   / res.link.) * res.link.
  e_ymin <- floor(min(coords[,2])   / res.link.) * res.link.
  e_xmax <- ceiling(max(coords[,1]) / res.link.) * res.link.
  e_ymax <- ceiling(max(coords[,2]) / res.link.) * res.link.

  # 3. Création du raster avec centres alignés
  xmin_pts <- min(coords[,1]);  xmax_pts <- max(coords[,1])
  ymin_pts <- min(coords[,2]);  ymax_pts <- max(coords[,2])

  first_center_x <- floor((xmin_pts   + res.link./2) / res.link.) * res.link.
  last_center_x  <- ceiling((xmax_pts - res.link./2) / res.link.) * res.link.
  first_center_y <- floor((ymin_pts   + res.link./2) / res.link.) * res.link.
  last_center_y  <- ceiling((ymax_pts - res.link./2) / res.link.) * res.link.

  ncols <- (last_center_x - first_center_x) / res.link. + 1
  nrows <- (last_center_y - first_center_y) / res.link. + 1

  xmin_raster <- first_center_x - res.link./2
  xmax_raster <- xmin_raster + ncols * res.link.
  ymin_raster <- first_center_y - res.link./2
  ymax_raster <- ymin_raster + nrows * res.link.

  r <- terra::rast(
    xmin = xmin_raster, xmax = xmax_raster,
    ymin = ymin_raster, ymax = ymax_raster,
    resolution = res.link.,
    crs = p4string
  )

  # 4. Comptage des cellules
  cells <- terra::cellFromXY(r, coords)
  tab   <- table(cells)

  # 5. Traitement PBL
  if (pbl.) {
    if (!is.null(rasterin)) {
      pbl_layer <- subset_nc_date(hpbl_brick = rasterin, vardate = d$Pdate[1])

      if (!inherits(pbl_layer, "SpatRaster"))
        pbl_layer <- terra::rast(pbl_layer)

      pbl_layer_proj <- terra::project(pbl_layer, r)

      pbl_values <- terra::extract(pbl_layer_proj,
                                   terra::xyFromCell(r, as.numeric(names(tab))))[, 1]

      vals <- as.numeric(tab) / pbl_values
      r[as.numeric(names(tab))] <- vals
    } else {
      warning("rasterin est NULL, utilisation sans PBL")
      r[as.numeric(names(tab))] <- as.numeric(tab)
    }
  } else {
    r[as.numeric(names(tab))] <- as.numeric(tab)
  }

  # 6. Trim et crop
  r_trimmed <- terra::trim(r)
  r2 <- terra::crop(r_trimmed, terra::ext(e_xmin, e_xmax, e_ymin, e_ymax))

  # 7. Crop USA si demandé
  if (crop.usa) {
    usa <- rnaturalearth::ne_countries(
      scale   = 110,
      type    = "countries",
      country = "United States of America",
      returnclass = "sf"
    )
    usa.sub <- sf::st_cast(usa, "POLYGON")[6,]
    usa.sub <- sf::st_transform(usa.sub, crs = p4string)
    r2 <- terra::crop(r2, terra::vect(usa.sub))
  }

  # 8. Retour grids
  xyz <- as.data.table(terra::as.data.frame(r2, xy = TRUE, na.rm = TRUE))
  names(xyz)[3] <- 'N'
  return(xyz)
}
