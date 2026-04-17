#' Link all units to grid spatial scale
#'
#' \code{link_all_units}
#'
#' @description with `link_all_units()` users can link all air parcels to grids
#' by month for specified units with combinations of years and months.
#' `link_all_units()` reads in all the relevant HYSPLIT files produced by
#' `run_disperser_parallel()` and saves them, then links them to grids.
#'
#' @param units.run A data.table with columns: ID (character), uID (character),
#' Latitude (numeric), Longitude (numeric), year (integer).
#'
#' @param year.mons months to link. Use `get_yearmon()` to create this vector.
#'
#' @param start.date optional start date (used instead of year.mons).
#' @param end.date optional end date (used instead of year.mons).
#'
#' @param pbl_trim logical. Trim parcel locations under monthly PBL heights?
#' @param pbl.height monthly boundary layer heights. Required if pbl_trim = TRUE.
#'
#' @param mc.cores number of cores for parallel processing.
#' @param duration.run.hours duration in hours (default 240 = 10 days).
#' @param res.link grid resolution in meters (default 12000 = 12 km).
#' @param overwrite overwrite existing output files?
#' @param crop.usa crop output to lower 48 US states?
#' @param return.linked.data return linked data in memory?
#'
#' @return data.table of grid-linked parcel data.
#'
#' @export link_all_units

link_all_units <- function(units.run,
                            link.to            = 'grids',
                            mc.cores           = detectCores(),
                            year.mons          = NULL,
                            start.date         = NULL,
                            end.date           = NULL,
                            pbl_trim           = TRUE,
                            pbl.height         = NULL,
                            duration.run.hours = 240,
                            res.link           = 12000,
                            overwrite          = FALSE,
                            pbl.trim           = FALSE,
                            crop.usa           = FALSE,
                            return.linked.data = TRUE) {

  if ((is.null(start.date) | is.null(end.date)) & is.null(year.mons))
    stop("Define either a start.date and an end.date OR a year.mons")
  if (link.to != 'grids')
    stop("link.to must be 'grids'")
  if (pbl_trim & is.null(pbl.height))
    stop("pbl.height must be provided if pbl_trim == TRUE")

  grids_link_parallel <- function(unit) {
    linked_grids <- parallel::mclapply(
      year.mons,
      disperser_link_grids,
      unit               = unit,
      pbl.height         = pbl.height,
      duration.run.hours = duration.run.hours,
      overwrite          = overwrite,
      res.link.          = res.link,
      mc.cores           = mc.cores,
      pbl.               = pbl.trim,
      crop.usa           = crop.usa,
      return.linked.data. = return.linked.data
    )

    linked_grids <- data.table::rbindlist(Filter(is.data.table, linked_grids))
    message(paste("processed unit", unit$ID, ""))

    linked_grids[, month := as(month, 'character')]
    return(linked_grids)
  }

  units.run <- unique(units.run[, list(uID, ID)])

  out <- units.run[, grids_link_parallel(.SD), by = seq_len(nrow(units.run))]

  out[, comb    := paste("month: ", out[, month], " unitID :", out[, ID], sep = "")]
  out[, seq_len := NULL]
  return(out)
}
