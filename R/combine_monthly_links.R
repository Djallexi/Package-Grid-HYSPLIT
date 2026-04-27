#' @description `combine_monthly_links()` combines linked files produced with
#' `link_all_units()` into lists of data.tables for easier manipulating
#'
#' @param month_YYYYMMs months and years to combine. Format created by `get_yearmon()`
#' @param link.to spatial scale for plotting. Only 'grids' supported.
#' @param filename What should the resulting RData file be called?
#'   Defaults to `paste0('hyads_unwgted_', link.to, '.RData')`
#'
#' @return Saves an .RData file to the rdata_dir defined by `create_dirs()`
#'   with filename `filename`.
#'
#' @export
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

    # FIX: deux sub() explicites plutôt qu'une alternation ambiguë
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

  # FIX: sortie propre si aucun mois n'a de données
  if (length(names.map) == 0) {
    warning("combine_monthly_links: aucune donnée trouvée — retourne NULL.")
    return(NULL)
  }

  # harmonisation de l'extent
 # harmonisation de l'extent — inutile si un seul mois
  out.d   <- mget(names.map)
  out.r   <- lapply(out.d, rasterFromXYZ)
  out.ids <- lapply(out.d, function(dt) names(dt))

  if (length(out.r) > 1) {
    out.e  <- extent(Reduce(extend, out.r))
    out.b  <- lapply(out.r, extend, out.e)
  } else {
    out.b  <- out.r          # rien à harmoniser
  }

  out.dt <- lapply(out.b, function(x) data.table(rasterToPoints(x)))
  out.dt <- lapply(
    out.dt,
    function(dt) dt[, `:=`(x = round(x), y = round(y))]
  )
  lapply(
    names(out.dt),
    function(x, l, n) {
      names(l[[x]]) <- out.ids[[x]]
      assign(x, l[[x]], envir = parent.env(environment()))
    },
    out.dt, out.ids
  )

  if (is.null(filename)) filename <- paste0("hyads_unwgted_", link.to, ".RData")
  rda.filename <- file.path(rdata_dir, filename)
  save(list = names.map, file = rda.filename)
  print(paste("Monthly RData file written to", rda.filename))
  return(mget(names.map))
}
