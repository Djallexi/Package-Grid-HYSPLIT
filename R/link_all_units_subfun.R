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
  output_file <- file.path(link_dir,
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
