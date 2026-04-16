plot_impact_sf_academic <- function(df,
                                    metric_col        = "N",
                                    lon_col           = "lon",
                                    lat_col           = "lat",
                                    uid_col    = "uID",      # ← NOUVEAU
                                    uid_filter = NULL,       # ← NOUVEAU : NULL = tous, sinon c(6, 12, ...)
                                    year              = NULL,      # ← NOUVEAU
                                    month             = NULL,      # ← NOUVEAU
                                    yearmonth_col     = "yearmonth", # ← NOUVEAU
                                    plant_locations   = NULL,
                                    map_data_custom   = NULL,
                                    padding           = 2,
                                    zoom              = TRUE,
                                    xlim_manual       = c(60, 100),
                                    ylim_manual       = c(5, 38),
                                    graph.dir         = NULL,
                                    plot.name         = expression(SO[2]~"Dispersion — Indian Coal Power Plants"),
                                    subtitle          = "Annual mean ground-level concentration (µg m⁻³)",
                                    file.name         = NULL,
                                    caption_txt       = "HYSPLIT Dispersion model output · WGS84",
                                    max_points        = 50000,
                                    percentile_limit  = 1,
                                    alpha             = 0.9,
                                    max_span_deg      = 10,
                                    color_palette     = "magma",
                                    scale_type        = "linear",
                                    scale_max_manual  = NULL,
                                    save_width        = 18,
                                    save_height       = 16,
                                    ...) {
  
  library(sf)
  library(ggplot2)
  library(maps)
  library(viridis)
  library(scales)
  library(ggspatial)
  
  df_work <- as.data.frame(df)
  
  # ── Filtrage par année / mois ─────────────────────────────────────────────────
  if (!is.null(year) || !is.null(month)) {
    if (!yearmonth_col %in% names(df_work))
      stop("Colonne '", yearmonth_col, "' introuvable dans df.")
    
    # yearmonth est au format YYYYMM (ex: 202201)
    ym_vals <- df_work[[yearmonth_col]]
    
    year_vals  <- as.integer(ym_vals) %/% 100L
    month_vals <- as.integer(ym_vals) %%  100L
    
    mask <- rep(TRUE, nrow(df_work))
    if (!is.null(year))  mask <- mask & (year_vals  == as.integer(year))
    if (!is.null(month)) mask <- mask & (month_vals == as.integer(month))
    
    df_work <- df_work[mask, , drop = FALSE]
    
    if (nrow(df_work) == 0)
      stop("Aucune ligne après filtrage : year=", year, ", month=", month)
    
    message(sprintf("Filtrage : %d lignes conservées (year=%s, month=%s)",
                    nrow(df_work),
                    ifelse(is.null(year),  "all", year),
                    ifelse(is.null(month), "all", month)))
  }
  
  # ── Filtrage par UID de centrale ──────────────────────────────────────────────
  if (!is.null(uid_filter)) {
    if (!uid_col %in% names(df_work))
      stop("Colonne '", uid_col, "' introuvable dans df.")
    
    df_work <- df_work[df_work[[uid_col]] %in% uid_filter, , drop = FALSE]
    
    if (nrow(df_work) == 0)
      stop("Aucune ligne après filtrage uid_filter = c(",
           paste(uid_filter, collapse = ", "), ")")
    
    message(sprintf("Filtrage uid : %d lignes conservées pour uid(s) : %s",
                    nrow(df_work),
                    paste(uid_filter, collapse = ", ")))
  }
  
  # ── Titre automatique si plot.name = NULL ─────────────────────────────────────
  if (is.null(plot.name)) {
    month_lbl <- if (!is.null(month))
      format(as.Date(paste0(ifelse(is.null(year), 2000, year), "-",
                            sprintf("%02d", as.integer(month)), "-01")), "%B %Y")
    else if (!is.null(year)) as.character(year)
    else "All periods"
    
    plot.name <- bquote(SO[2] ~ "Dispersion —" ~ .(month_lbl))
  }
  
  # ── Nom de fichier automatique si file.name = NULL ───────────────────────────
  if (is.null(file.name)) {
    y_str <- ifelse(is.null(year),  "allY", year)
    m_str <- ifelse(is.null(month), "allM", sprintf("%02d", as.integer(month)))
    file.name <- paste0(metric_col, "_", color_palette, "_", scale_type,
                        "_", y_str, m_str, ".png")
  }
  
  # ── Vérifications colonnes ────────────────────────────────────────────────────
  for (col in c(lon_col, lat_col, metric_col)) {
    if (!col %in% names(df_work))
      stop("Colonne manquante dans df : '", col, "'")
  }
  
  keep <- is.finite(df_work[[lon_col]]) &
    is.finite(df_work[[lat_col]])   &
    is.finite(df_work[[metric_col]])
  df_work <- df_work[keep, , drop = FALSE]
  if (nrow(df_work) == 0) stop("Aucune ligne valide (lon/lat/metric finis).")
  
  df_work$geometry <- mapply(
    function(x, y) sf::st_point(c(x, y)),
    df_work[[lon_col]], df_work[[lat_col]],
    SIMPLIFY = FALSE
  )
  if (metric_col != "metric") df_work$metric <- df_work[[metric_col]]
  
  # ── Logique originale inchangée ───────────────────────────────────────────────
  geom_raw  <- df_work[["geometry"]]
  valid_idx <- sapply(geom_raw, function(x) inherits(x, "sfg") && !any(is.na(unlist(x))))
  if (sum(valid_idx) == 0) stop("No valid sfg geometries.")
  
  gdf <- st_sf(
    metric   = df_work[["metric"]][valid_idx],
    geometry = st_sfc(geom_raw[valid_idx], crs = 4326)
  )
  
  upper_lim <- quantile(gdf$metric, percentile_limit, na.rm = TRUE)
  gdf <- gdf[gdf$metric >= 0 & gdf$metric <= upper_lim, ]
  if (nrow(gdf) == 0) stop("Aucun point après filtrage.")
  if (nrow(gdf) > max_points) gdf <- gdf[sort(sample(nrow(gdf), max_points)), ]
  
  scale_type <- match.arg(scale_type, c("linear", "log", "quantile", "manual"))
  
  if (scale_type == "log") {
    gdf$metric_plot <- log1p(gdf$metric)
    value_col       <- "metric_plot"
    scale_lims      <- c(0, max(gdf$metric_plot, na.rm = TRUE))
    scale_lab       <- paste0("log(1+", metric_col, ")")
    orig_breaks     <- c(0, 1, 5, 10, 50, 100, 500, 1000)
    orig_breaks     <- orig_breaks[orig_breaks <= max(gdf$metric, na.rm = TRUE)]
    break_vals      <- log1p(orig_breaks)
    break_labs      <- as.character(orig_breaks)
  } else if (scale_type == "quantile") {
    probs           <- seq(0, 1, length.out = 256)
    qtiles          <- quantile(gdf$metric, probs, na.rm = TRUE)
    gdf$metric_plot <- findInterval(gdf$metric, qtiles) / 256
    value_col       <- "metric_plot"
    scale_lims      <- c(0, 1)
    scale_lab       <- paste0(metric_col, " (quantile)")
    key_probs       <- c(0, 0.25, 0.5, 0.75, 0.90, 0.95, 1)
    break_vals      <- key_probs
    break_labs      <- format(round(quantile(gdf$metric, key_probs, na.rm = TRUE)), big.mark = ",")
  } else if (scale_type == "manual") {
    if (is.null(scale_max_manual)) {
      scale_max_manual <- quantile(gdf$metric, 0.90, na.rm = TRUE)
      message("scale_max_manual non fourni, utilisation du percentile 90% : ",
              round(scale_max_manual))
    }
    gdf$metric_plot <- gdf$metric
    value_col       <- "metric_plot"
    scale_lims      <- c(0, scale_max_manual)
    scale_lab       <- metric_col
    break_vals      <- NULL
    break_labs      <- NULL
  } else {
    gdf$metric_plot <- gdf$metric
    value_col       <- "metric_plot"
    scale_lims      <- c(0, quantile(gdf$metric, 0.90, na.rm = TRUE))
    scale_lab       <- metric_col
    break_vals      <- NULL
    break_labs      <- NULL
  }
  
  map_use <- if (!is.null(map_data_custom)) {
    if (inherits(map_data_custom, "sf")) st_transform(map_data_custom, 4326)
    else st_transform(st_read(map_data_custom, quiet = TRUE), 4326)
  } else {
    st_as_sf(maps::map("world", plot = FALSE, fill = TRUE))
  }
  
  if (zoom) { xl <- xlim_manual; yl <- ylim_manual
  } else    { xl <- c(-180, 180); yl <- c(-90, 90) }
  
  cbar <- guide_colorbar(
    barwidth = unit(0.45, "cm"), barheight = unit(5.5, "cm"),
    title.position = "top", title.hjust = 0.5,
    ticks.colour = "white", frame.colour = "grey40"
  )
  
  make_color_scale <- function(pal, lims, lab, bvals, blabs) {
    base_args <- list(name = lab, limits = lims, oob = squish,
                      na.value = "transparent", guide = cbar)
    if (!is.null(bvals)) { base_args$breaks <- bvals; base_args$labels <- blabs }
    switch(pal,
           "magma"    = do.call(scale_color_viridis_c, c(base_args, option = "magma")),
           "plasma"   = do.call(scale_color_viridis_c, c(base_args, option = "plasma")),
           "inferno"  = do.call(scale_color_viridis_c, c(base_args, option = "inferno")),
           "viridis"  = do.call(scale_color_viridis_c, c(base_args, option = "viridis")),
           "hotspot"  = do.call(scale_color_gradientn, c(base_args, list(colours = c(
             "#0d0221","#3d0f6e","#8b1a8b","#d44000","#f97316","#fbbf24","#fef08a")))),
           "sentinel" = do.call(scale_color_gradientn, c(base_args, list(colours = c(
             "#03051a","#0a2472","#0e6ba8","#a8dadc","#f4d35e","#f95738","#6b0504")))),
           do.call(scale_color_distiller, c(base_args, palette = "YlOrRd", direction = 1))
    )
  }
  
  scale_info    <- switch(scale_type,
                          "log"      = " | échelle logarithmique",
                          "quantile" = " | échelle quantile",
                          "manual"   = paste0(" | max = ", format(round(scale_lims[2]), big.mark = ",")),
                          "")
  subtitle_full <- paste0(subtitle, scale_info)
  
  gg <- ggplot() +
    geom_sf(data = gdf, aes(color = .data[[value_col]]),
            size = 0.3, shape = 15, alpha = alpha) +
    make_color_scale(color_palette, scale_lims, scale_lab, break_vals, break_labs) +
    geom_sf(data = map_use, fill = NA, color = "grey70", linewidth = 0.3) +
    {
      if (!is.null(plant_locations)) {
        pl <- plant_locations[plant_locations$capacity_mw > 0, ]
        list(
          geom_point(data = pl, aes(x = lon, y = lat,
                                    size = if ("capacity_mw" %in% names(pl)) capacity_mw else 2),
                     shape = 21, fill = "white", color = "black", stroke = 0.8, alpha = 0.95),
          scale_size_continuous(name = "Capacity\n(MW)", range = c(0.5, 2.5),
                                guide = guide_legend(override.aes = list(fill = "white", color = "black")))
        )
      }
    } +
    {tryCatch(list(
      annotation_north_arrow(location = "tl", which_north = "true",
                             pad_x = unit(0.4,"cm"), pad_y = unit(0.5,"cm"),
                             style = north_arrow_fancy_orienteering(fill = c("grey80","white"),
                                                                    line_col = "grey60", text_col = "white", text_size = 7),
                             height = unit(1.2,"cm"), width = unit(1.2,"cm")),
      annotation_scale(location = "bl", width_hint = 0.2,
                       text_cex = 0.7, line_col = "grey70",
                       text_col = "grey80", bar_cols = c("grey60","grey20"))
    ), error = function(e) list())} +
    coord_sf(xlim = xl, ylim = yl, crs = st_crs(4326), expand = FALSE) +
    labs(title = plot.name, subtitle = subtitle_full, caption = caption_txt) +
    theme_bw(base_size = 10, base_family = "serif") +
    theme(
      plot.background   = element_rect(fill = "#0a0a0a", color = NA),
      panel.background  = element_rect(fill = "#0d1b2a"),
      panel.border      = element_rect(color = "grey40", linewidth = 0.5, fill = NA),
      panel.grid.major  = element_line(color = alpha("white", 0.08), linewidth = 0.2, linetype = "dotted"),
      panel.grid.minor  = element_blank(),
      plot.title        = element_text(size = 13, face = "bold", color = "white",
                                       hjust = 0, margin = margin(b = 3)),
      plot.subtitle     = element_text(size = 9, color = "grey60",
                                       hjust = 0, margin = margin(b = 6)),
      plot.caption      = element_text(size = 7, color = "grey50",
                                       hjust = 1, margin = margin(t = 5)),
      plot.margin       = margin(12, 18, 8, 12),
      axis.text         = element_text(size = 7.5, color = "grey60"),
      axis.ticks        = element_line(color = "grey50", linewidth = 0.3),
      axis.title        = element_blank(),
      legend.background = element_rect(fill = alpha("#1a1a2e", 0.9), color = "grey40", linewidth = 0.3),
      legend.text       = element_text(size = 7.5, color = "grey80"),
      legend.title      = element_text(size = 8, face = "bold", color = "white"),
      legend.margin     = margin(6, 8, 6, 8),
      ...
    )
  
  cat("Statistiques de", metric_col, "après filtrage :\n")
  cat("N total (somme) :", format(round(sum(df_work[[metric_col]], na.rm = TRUE)), big.mark = ","), "\n")
  print(summary(df_work[[metric_col]]))
  cat("Nombre de cellules avec N > 100 :", sum(df_work[[metric_col]] > 100), "\n")
  cat("Valeur max :", max(df_work[[metric_col]]), "\n")
  
  
  if (!is.null(graph.dir)) {
    if (!dir.exists(graph.dir)) dir.create(graph.dir, recursive = TRUE)
    path <- file.path(graph.dir, file.name)
    ggsave(gg, filename = path, width = save_width, height = save_height,
           units = "cm", dpi = 300, bg = "#0a0a0a")
    message("Saved: ", path)
  }
  
  return(gg)
}