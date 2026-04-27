combine_monthly_links <- function(
  month_YYYYMMs,
  link.to = "grids",
  filename = NULL
) {

  names.map <- c()

  for (ym in month_YYYYMMs) {

    year.h  <- substr(ym, 1, 4)
    month.m <- as.integer(substr(ym, 5, 6))
    month.h <- formatC(month.m, width = 2, format = "d", flag = "0")

    if (link.to == "grids") {

      pattern <- paste0("gridlinks.*", year.h, "-", month.h, ".*\\.fst$")

      files.month <- list.files(
        path = links_dir,
        pattern = pattern,
        full.names = TRUE
      )

      if (length(files.month) == 0) {
        print(paste("No data files for", ym))
        next
      }

      print(paste("Reading and merging month", month.h, "year", year.h))

      unitnames <- gsub(
        paste0(".*gridlinks_|_", year.h, "-", month.h, ".*fst$"),
        "",
        files.month
      )

      names(files.month) <- unitnames

      data.h <- lapply(
        seq_along(files.month),
        read_gridlinks_subfun,
        files.month
      )

      MergedDT <- rbindlist(data.h)

      Merged_cast <- dcast(
        MergedDT,
        x + y ~ ID,
        fun.aggregate = sum,
        value.var = "N"
      )

      name.map <- paste0("MAP", month.m, ".", year.h)
      names.map <- append(names.map, name.map)

      assign(name.map, Merged_cast)
      rm(MergedDT, Merged_cast)
    }
  }

  # harmonisation de l’extent si grids
  if (link.to == "grids") {

    out.d   <- mget(names.map)
    out.r   <- lapply(out.d, rasterFromXYZ)
    out.ids <- lapply(out.d, function(dt) names(dt))

    out.e <- extent(Reduce(extend, out.r))

    out.b <- lapply(out.r, extend, out.e)

    out.dt <- lapply(out.b, function(x) data.table(rasterToPoints(x)))

    out.dt <- lapply(
      out.dt,
      function(dt) dt[, `:=`(
        x = round(x),
        y = round(y)
      )]
    )

    lapply(
      names(out.dt),
      function(x, l, n) {
        names(l[[x]]) <- out.ids[[x]]
        assign(x, l[[x]], envir = parent.env(environment()))
      },
      out.dt, out.ids
    )
  }

  if (is.null(filename)) {
    filename <- paste0("hyads_unwgted_", link.to, ".RData")
  }

  rda.filename <- file.path(rdata_dir, filename)
  save(list = names.map, file = rda.filename)

  print(paste("Monthly RData file written to", rda.filename))

  return(mget(names.map))
}
