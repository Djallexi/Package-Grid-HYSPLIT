plot_impact_sf_academic <- function(df,
                                    metric_col       = "N",
                                    lon_col          = "lon",
                                    lat_col          = "lat",
                                    uid_col          = "uID",
                                    uid_filter       = NULL,
                                    year             = NULL,
                                    month            = NULL,
                                    yearmonth_col    = "yearmonth",
                                    plant_locations  = NULL,
                                    map_data_custom  = NULL,
                                    zoom             = TRUE,
                                    xlim_manual      = c(60, 100),
                                    ylim_manual      = c(5, 38),
                                    graph.dir        = NULL,
                                    plot.name        = expression(SO[2]~"Dispersion — Indian Coal Power Plants"),
                                    subtitle         = "Annual mean ground-level concentration (µg m⁻³)",
                                    file.name        = NULL,
                                    caption_txt      = "HYSPLIT Dispersion model output · WGS84",
                                    percentile_limit    = 1,
                                    percentile_limit_lo = 0, 
                                    metric_min_manual   = NULL, # borne absolue, ex : -1
                                    alpha            = 0.9,
                                    color_palette    = "magma",
                                    scale_type       = "linear",
                                    scale_max_manual = NULL,
                                    cell_size        = 0.1,
                                    save_width       = 18,
                                    save_height      = 16,
                                    ...) {
  
  library(sf)
  library(ggplot2)
  library(maps)
  library(viridis)
  library(scales)
  library(ggspatial)
  library(data.table)
  
  df_work <- as.data.table(df)
  
  # ── Filtrage par année / mois ──────────────────────────────────────────────
  if (!is.null(year) || !is.null(month)) {
    
    has_ym  <- yearmonth_col %in% names(df_work)
    has_sep <- all(c("year", "month") %in% names(df_work))
    
    if (has_ym) {
      ym_vals    <- df_work[[yearmonth_col]]
      year_vals  <- as.integer(substr(as.character(ym_vals), 1, 4))
      month_vals <- as.integer(substr(as.character(ym_vals), 5, 6))
    } else if (has_sep) {
      year_vals  <- as.integer(df_work[["year"]])
      month_vals <- as.integer(df_work[["month"]])
      message("Colonnes séparées 'year' et 'month' détectées.")
    } else {
      stop("Impossible de filtrer par date : ni '", yearmonth_col,
           "' ni 'year'+'month' présentes dans df.")
    }
    
    mask <- rep(TRUE, nrow(df_work))
    if (!is.null(year))  mask <- mask & (year_vals  == as.integer(year))
    if (!is.null(month)) mask <- mask & (month_vals == as.integer(month))
    
    df_work <- df_work[mask]
    
    if (nrow(df_work) == 0)
      stop("Aucune ligne après filtrage : year=", year, ", month=", month)
    
    message(sprintf("Filtrage : %s lignes conservées (year=%s, month=%s)",
                    format(nrow(df_work), big.mark = ","),
                    ifelse(is.null(year),  "all", as.character(year)),
                    ifelse(is.null(month), "all", as.character(month))))
  }
  
  # ── Filtrage par UID ───────────────────────────────────────────────────────
  if (!is.null(uid_filter)) {
    if (!uid_col %in% names(df_work))
      stop("Colonne '", uid_col, "' introuvable dans df.")
    df_work <- df_work[df_work[[uid_col]] %in% uid_filter]
    if (nrow(df_work) == 0)
      stop("Aucune ligne après filtrage uid_filter.")
    message(sprintf("Filtrage uid : %s lignes conservées.",
                    format(nrow(df_work), big.mark = ",")))
  }
  
  # ── Titre / fichier auto ───────────────────────────────────────────────────
  if (is.null(plot.name)) {
    month_lbl <- if (!is.null(month))
      format(as.Date(paste0(ifelse(is.null(year), 2000, year), "-",
                            sprintf("%02d", as.integer(month)), "-01")), "%B %Y")
    else if (!is.null(year)) as.character(year)
    else "All periods"
    plot.name <- bquote(SO[2] ~ "Dispersion —" ~ .(month_lbl))
  }
  
  if (is.null(file.name)) {
    y_str <- ifelse(is.null(year),  "allY", year)
    m_str <- ifelse(is.null(month), "allM", sprintf("%02d", as.integer(month)))
    file.name <- paste0(metric_col, "_", color_palette, "_", scale_type,
                        "_", y_str, m_str, ".png")
  }
  
  # ── Vérifications colonnes ─────────────────────────────────────────────────
  for (col in c(lon_col, lat_col, metric_col))
    if (!col %in% names(df_work))
      stop("Colonne manquante : '", col, "'")
  
  # ── Nettoyage + renommage interne ──────────────────────────────────────────
  df_work <- df_work[
    is.finite(df_work[[lon_col]]) &
      is.finite(df_work[[lat_col]]) &
      is.finite(df_work[[metric_col]])
  ]
  if (nrow(df_work) == 0) stop("Aucune ligne valide (lon/lat/metric finis).")
  
# ── Nettoyage + renommage interne ──────────────────────────────────────────
  df_work[, `:=`(
    .lon    = get(lon_col),
    .lat    = get(lat_col),
    .metric = get(metric_col)
  )]
  
  # ── Agréger par cellule (moyenne si plusieurs valeurs par cellule) ─────────
  df_plot <- df_work[, .(.metric = mean(.metric, na.rm = TRUE)),
                     by = .(.lon, .lat)]
  
  # ── Troncature percentile ──────────────────────────────────────────────────
  lower_lim <- if (!is.null(metric_min_manual)) {
    metric_min_manual
  } else {
    quantile(df_plot$.metric, percentile_limit_lo, na.rm = TRUE)
  }
  
  upper_lim <- quantile(df_plot$.metric, percentile_limit, na.rm = TRUE)
  df_plot   <- df_plot[.metric >= lower_lim & .metric <= upper_lim]
  if (nrow(df_plot) == 0) stop("Aucun point après filtrage percentile.")
  
  cat(sprintf("\nStatistiques de '%s' :\n", metric_col))
  print(summary(df_plot$.metric))
  cat(sprintf("Cellules      : %s\n", format(nrow(df_plot), big.mark = ",")))
  cat(sprintf("Max           : %.4f\n", max(df_plot$.metric, na.rm = TRUE)))
  
  # ── Échelle ────────────────────────────────────────────────────────────────
  scale_type <- match.arg(scale_type, c("linear", "log", "quantile", "manual"))
  
  if (scale_type == "log") {
    df_plot[, .value := log1p(.metric)]
    scale_lims <- c(lower_lim, quantile(df_plot$.metric, 0.90, na.rm = TRUE))
    scale_lab   <- paste0("log(1+", metric_col, ")")
    orig_breaks <- c(0, 1, 5, 10, 50, 100, 500, 1000, 5000)
    orig_breaks <- orig_breaks[orig_breaks <= max(df_plot$.metric, na.rm = TRUE)]
    break_vals  <- log1p(orig_breaks)
    break_labs  <- as.character(orig_breaks)
    
  } else if (scale_type == "quantile") {
    probs      <- seq(0, 1, length.out = 256)
    qtiles     <- quantile(df_plot$.metric, probs, na.rm = TRUE)
    df_plot[, .value := findInterval(.metric, qtiles) / 256]
    scale_lims <- c(0, 1)
    scale_lab  <- paste0(metric_col, " (quantile)")
    key_probs  <- c(0, 0.25, 0.5, 0.75, 0.90, 0.95, 1)
    break_vals <- key_probs
    break_labs <- format(round(quantile(df_plot$.metric, key_probs, na.rm = TRUE)),
                         big.mark = ",")
    
  } else if (scale_type == "manual") {
    if (is.null(scale_max_manual)) {
      scale_max_manual <- quantile(df_plot$.metric, 0.90, na.rm = TRUE)
      message("scale_max_manual auto : ", round(scale_max_manual))
    }
    df_plot[, .value := .metric]
    scale_lims <- c(lower_lim, scale_max_manual)  # ← corrigé
    scale_lab  <- metric_col
    break_vals <- NULL; break_labs <- NULL
    
  } else {  # linear
    df_plot[, .value := .metric]
    has_neg    <- any(df_plot$.metric < 0, na.rm = TRUE)
    scale_lims <- if (has_neg)
      c(quantile(df_plot$.metric, 0.10, na.rm = TRUE),
        quantile(df_plot$.metric, 0.90, na.rm = TRUE))
    else
      c(0, quantile(df_plot$.metric, 0.90, na.rm = TRUE))
    scale_lab  <- metric_col
    break_vals <- NULL; break_labs <- NULL
  }
  
  # ── Fond cartographique ────────────────────────────────────────────────────
  map_use <- if (!is.null(map_data_custom)) {
    if (inherits(map_data_custom, "sf")) st_transform(map_data_custom, 4326)
    else st_transform(st_read(map_data_custom, quiet = TRUE), 4326)
  } else {
    st_as_sf(maps::map("world", plot = FALSE, fill = TRUE))
  }
  
  xl <- if (zoom) xlim_manual else c(-180, 180)
  yl <- if (zoom) ylim_manual else c(-90, 90)
  
  # ── Palette couleur (fill) ─────────────────────────────────────────────────
  cbar <- guide_colorbar(
    barwidth  = unit(0.45, "cm"), barheight = unit(5.5, "cm"),
    title.position = "top", title.hjust = 0.5,
    ticks.colour = "white", frame.colour = "grey40"
  )
  
  make_fill_scale <- function(pal, lims, lab, bvals, blabs) {
    base <- list(name = lab, limits = lims, oob = squish,
                 na.value = "transparent", guide = cbar)
    if (!is.null(bvals)) { base$breaks <- bvals; base$labels <- blabs }
    switch(pal,
           "magma"    = do.call(scale_fill_viridis_c, c(base, option = "magma")),
           "plasma"   = do.call(scale_fill_viridis_c, c(base, option = "plasma")),
           "inferno"  = do.call(scale_fill_viridis_c, c(base, option = "inferno")),
           "viridis"  = do.call(scale_fill_viridis_c, c(base, option = "viridis")),
           "hotspot"  = do.call(scale_fill_gradientn, c(base, list(colours = c(
             "#0d0221","#3d0f6e","#8b1a8b","#d44000","#f97316","#fbbf24","#fef08a")))),
           "sentinel" = do.call(scale_fill_gradientn, c(base, list(colours = c(
             "#03051a","#0a2472","#0e6ba8","#a8dadc","#f4d35e","#f95738","#6b0504")))),
           do.call(scale_fill_distiller, c(base, palette = "YlOrRd", direction = 1))
    )
  }
  
  scale_info    <- switch(scale_type,
                          "log"      = " | échelle logarithmique",
                          "quantile" = " | échelle quantile",
                          "manual"   = paste0(" | max = ", format(round(scale_lims[2]), big.mark = ",")),
                          "")
  subtitle_full <- paste0(subtitle, scale_info)
  
  # ── Plot ───────────────────────────────────────────────────────────────────

    gg <- ggplot() +
    geom_raster(
      data    = df_plot,
      aes(x = .lon, y = .lat, fill = .value),
      alpha   = alpha
    ) +
    make_fill_scale(color_palette, scale_lims, scale_lab, break_vals, break_labs) +
    geom_sf(data = map_use, fill = NA, color = "grey70", linewidth = 0.3) +
    {
      if (!is.null(plant_locations)) {
        pl <- plant_locations[plant_locations$capacity_mw > 0, ]
        list(
          geom_point(
            data  = pl,
            aes(x = lon, y = lat,
                size = if ("capacity_mw" %in% names(pl)) capacity_mw else 2),
            shape = 21, fill = "white", color = "black", stroke = 0.8, alpha = 0.95
          ),
          scale_size_continuous(
            name  = "Capacity\n(MW)", range = c(0.5, 2.5),
            guide = guide_legend(override.aes = list(fill = "white", color = "black"))
          )
        )
      }
    } +
    {
      tryCatch(list(
        annotation_north_arrow(
          location = "tl", which_north = "true",
          pad_x = unit(0.4, "cm"), pad_y = unit(0.5, "cm"),
          style  = north_arrow_fancy_orienteering(
            fill = c("grey80", "white"), line_col = "grey60",
            text_col = "white", text_size = 7),
          height = unit(1.2, "cm"), width = unit(1.2, "cm")
        ),
        annotation_scale(
          location   = "bl", width_hint = 0.2,
          text_cex   = 0.7, line_col = "grey70",
          text_col   = "grey80", bar_cols = c("grey60", "grey20")
        )
      ), error = function(e) list())
    } +
    coord_sf(xlim = xl, ylim = yl, crs = st_crs(4326), expand = FALSE) +
    labs(title = plot.name, subtitle = subtitle_full, caption = caption_txt) +
    theme_bw(base_size = 10, base_family = "serif") +
    theme(
      plot.background  = element_rect(fill = "#0a0a0a", color = NA),
      panel.background = element_rect(fill = "#0d1b2a"),
      panel.border     = element_rect(color = "grey40", linewidth = 0.5, fill = NA),
      panel.grid.major = element_line(color = alpha("white", 0.08),
                                      linewidth = 0.2, linetype = "dotted"),
      panel.grid.minor = element_blank(),
      plot.title       = element_text(size = 13, face = "bold", color = "white",
                                      hjust = 0, margin = margin(b = 3)),
      plot.subtitle    = element_text(size = 9, color = "grey60",
                                      hjust = 0, margin = margin(b = 6)),
      plot.caption     = element_text(size = 7, color = "grey50",
                                      hjust = 1, margin = margin(t = 5)),
      plot.margin      = margin(12, 18, 8, 12),
      axis.text        = element_text(size = 7.5, color = "grey60"),
      axis.ticks       = element_line(color = "grey50", linewidth = 0.3),
      axis.title       = element_blank(),
      legend.background = element_rect(fill = alpha("#1a1a2e", 0.9),
                                       color = "grey40", linewidth = 0.3),
      legend.text      = element_text(size = 7.5, color = "grey80"),
      legend.title     = element_text(size = 8, face = "bold", color = "white"),
      legend.margin    = margin(6, 8, 6, 8),
      ...
    )
  
  # ── Save ───────────────────────────────────────────────────────────────────
  if (!is.null(graph.dir)) {
    if (!dir.exists(graph.dir)) dir.create(graph.dir, recursive = TRUE)
    path <- file.path(graph.dir, file.name)
    ggsave(gg, filename = path,
           width = save_width, height = save_height,
           units = "cm", dpi = 300, bg = "#0a0a0a")
    message("Saved: ", path)
  }
  
  return(gg)
}
