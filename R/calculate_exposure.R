#' @export calculate_exposure
#'
#' OPTIMISATIONS (résultats strictement identiques) :
#'   O1 — Encodage uID entier  -> join ~10x plus rapide que character
#'   O2 — Join unique vectorisé -> supprime split/mclapply/rbindlist
#'   O3 — Pré-agrégation doublons avant join
#'   + Progression détaillée à chaque sous-étape

calculate_exposure <- function(year.E,
                               year.D,
                               link.to    = 'grids',
                               pollutant  = 'SO2_month',
                               units.mo,
                               linked_grids,
                               exp_dir    = NULL,
                               source.agg = c('total', 'facility', 'unit'),
                               time.agg   = c('year', 'month'),
                               return.monthly.data = FALSE,
                               cores = parallel::detectCores() - 1L) {
  
  `%ni%` <- Negate(`%in%`)
  
  # ── Helpers ────────────────────────────────────────────────────────────────
  t_start <- proc.time()
  
  tick <- function(label, t0 = NULL) {
    elapsed_total <- round((proc.time() - t_start)["elapsed"], 1)
    if (!is.null(t0))
      elapsed_step <- round((proc.time() - t0)["elapsed"], 1)
    else
      elapsed_step <- NA
    if (!is.na(elapsed_step))
      cat(sprintf("    ✓ %s  [+%.1fs | total %.1fs]\n", label, elapsed_step, elapsed_total))
    else
      cat(sprintf("    ✓ %s  [total %.1fs]\n", label, elapsed_total))
  }
  
  step <- function(n, total, label) {
    cat(sprintf("\n[%d/%d] %s\n", n, total, label))
    proc.time()
  }
  
  pb_chunked <- function(x_vec, fun, chunk_size = 5e6, label = "") {
    # Applique fun() par chunk de chunk_size lignes avec barre de progression
    n      <- length(x_vec)
    chunks <- split(seq_len(n), ceiling(seq_len(n) / chunk_size))
    pb     <- txtProgressBar(min = 0, max = length(chunks), style = 3, width = 55)
    result <- vector("list", length(chunks))
    for (i in seq_along(chunks)) {
      result[[i]] <- fun(x_vec[chunks[[i]]])
      setTxtProgressBar(pb, i)
    }
    close(pb); cat("\n")
    unlist(result, use.names = FALSE)
  }
  
  N_STEPS <- 5L
  
  # ── 1. Validation ──────────────────────────────────────────────────────────
  if (length(source.agg) > 1) { message('source.agg defaulting to "total".'); source.agg <- 'total' }
  if (source.agg %ni% c('total', 'facility', 'unit'))
    stop('source.agg must be one of c("total", "facility", "unit").')
  if (length(time.agg) > 1) { message('time.agg defaulting to "year".'); time.agg <- 'year' }
  if (time.agg %ni% c('year', 'month'))
    stop('time.agg must be one of c("year", "month").')
  
  old_threads <- data.table::getDTthreads()
  data.table::setDTthreads(cores)
  arrow::set_cpu_count(cores)
  on.exit({ data.table::setDTthreads(old_threads); cat("\n") }, add = TRUE)
  
  message(sprintf("\n  Threads data.table : %d  |  Cores : %d\n", cores, cores))
  
  # ── 2. Préparer units.mo ──────────────────────────────────────────────────
  t0 <- step(1L, N_STEPS, "Préparation units.mo")
  
  um <- as.data.table(units.mo)
  um[, uID := as.character(uID)]
  if (!'month' %in% names(um)) um[, month := 1L]
  
  extra_cols <- intersect('FacID', names(um))
  keep_cols  <- unique(c('uID', 'month', pollutant, extra_cols))
  
  um_E <- um[year == year.E, .SD, .SDcols = keep_cols]
  setnames(um_E, pollutant, 'pollutant')
  um_E[is.na(pollutant), pollutant := 0]
  
  valid_uids <- unique(um_E$uID)
  tick(sprintf("%d unités valides (year.E=%d)", length(valid_uids), year.E), t0)
  
  # ── 3. Préparer linked_grids ──────────────────────────────────────────────
  t0 <- step(2L, N_STEPS, sprintf("Préparation linked_grids (%s lignes)",
                                  format(nrow(linked_grids), big.mark = ",")))
  setDT(linked_grids)
  
  if ('ID' %in% names(linked_grids) && !'uID' %in% names(linked_grids))
    setnames(linked_grids, 'ID', 'uID')
  
  # Conversions in-place avec chrono individuel
  t1 <- proc.time()
  cat("  as.character(uID)... ")
  set(linked_grids, j = 'uID', value = as.character(linked_grids$uID))
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  t1 <- proc.time()
  cat("  round(x,1) + round(y,1)... ")
  set(linked_grids, j = 'x', value = round(linked_grids$x, 1))
  set(linked_grids, j = 'y', value = round(linked_grids$y, 1))
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  t1 <- proc.time()
  cat("  month_num... ")
  if (max(linked_grids$month, na.rm = TRUE) > 12L) {
    set(linked_grids, j = 'month_num',
        value = as.integer(substr(as.character(linked_grids$month), 5, 6)))
  } else {
    set(linked_grids, j = 'month_num', value = as.integer(linked_grids$month))
  }
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  # Filtre uID
  t1 <- proc.time()
  cat("  Filtre uID valides... ")
  n_before <- nrow(linked_grids)
  linked_grids <- linked_grids[uID %in% valid_uids, .(x, y, uID, month_num, N)]
  cat(sprintf("%.1fs  |  %s -> %s lignes (-%d%%)\n",
              (proc.time() - t1)["elapsed"],
              format(n_before,           big.mark = ","),
              format(nrow(linked_grids), big.mark = ","),
              round(100 * (1 - nrow(linked_grids) / n_before))))
  
  # O3 : Pré-agrégation doublons
  t1 <- proc.time()
  cat("  [O3] Pré-agrégation (x, y, uID, month_num)... ")
  linked_grids <- linked_grids[, .(N = sum(N, na.rm = TRUE)),
                               by = .(x, y, uID, month_num)]
  cat(sprintf("%.1fs  |  %s lignes\n",
              (proc.time() - t1)["elapsed"],
              format(nrow(linked_grids), big.mark = ",")))
  
  # O1 : Encodage integer
  t1 <- proc.time()
  cat("  [O1] Encodage uID -> entier... ")
  uid_levels <- unique(c(linked_grids$uID, um_E$uID))
  set(linked_grids, j = 'uid_int', value = match(linked_grids$uID, uid_levels))
  set(um_E,         j = 'uid_int', value = match(um_E$uID,         uid_levels))
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  tick("Préparation linked_grids terminée", t0)
  
  # ── 4. Join vectorisé ─────────────────────────────────────────────────────
  t0 <- step(3L, N_STEPS, "Join vectorisé unique (data.table multi-thread)")
  
  um_join_cols <- unique(c('uid_int', 'month', 'pollutant', extra_cols))
  um_join <- um_E[, .SD, .SDcols = um_join_cols]
  
  t1 <- proc.time()
  cat("  setkey(um_join)... ")
  setkey(um_join, uid_int, month)
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  t1 <- proc.time()
  cat("  setkey(linked_grids)... ")
  setkey(linked_grids, uid_int, month_num)
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  t1 <- proc.time()
  cat("  Inner join (uid_int x month)... ")
  joined <- um_join[linked_grids,
                    on      = .(uid_int, month = month_num),
                    nomatch = 0L]
  cat(sprintf("%.1fs  |  %s lignes jointes\n",
              (proc.time() - t1)["elapsed"],
              format(nrow(joined), big.mark = ",")))
  
  t1 <- proc.time()
  cat("  Calcul Exposure = pollutant * N... ")
  set(joined, j = 'Exposure', value = joined$pollutant * joined$N)
  joined[, c('uid_int', 'pollutant', 'N') := NULL]
  cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
  
  tick("Join + exposition terminés", t0)
  
  # Colonnes de regroupement
  id.v <- switch(link.to,
                 grids    = c('x', 'y'),
                 zips     = 'ZIP',
                 counties = c("statefp", "countyfp", "state_name", "name", "geoid"),
                 stop("link.to non reconnu."))
  
  sum.by.base <- switch(source.agg,
                        total    = id.v,
                        facility = c(id.v, 'FacID'),
                        unit     = c(id.v, 'uID'))
  
  file.by <- switch(source.agg,
                    total    = '_exposures_total_',
                    facility = '_exposures_byfacility_',
                    unit     = '_exposures_byunit_')
  
  # ── 5. Agrégation finale ──────────────────────────────────────────────────
  t0 <- step(4L, N_STEPS, "Agrégation finale")
  
  if (time.agg == 'year') {
    t1 <- proc.time()
    cat("  sum(Exposure) by (x, y, [uID])... ")
    exposures <- joined[,
                        .(hyads = sum(Exposure, na.rm = TRUE)),
                        by = sum.by.base
    ][hyads > 0]
    exposures[, `:=`(year.E = year.E, year.D = year.D)]
    cat(sprintf("%.1fs  |  %s lignes\n",
                (proc.time() - t1)["elapsed"],
                format(nrow(exposures), big.mark = ",")))
    
  } else {
    t1 <- proc.time()
    cat("  Construction yearmonth... ")
    set(joined, j = 'yearmonth',
        value = paste0(year.E, formatC(joined$month, width = 2, flag = '0')))
    cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
    
    t1 <- proc.time()
    cat("  sum(Exposure) by (x, y, [uID], yearmonth)... ")
    exposures <- joined[,
                        .(hyads = sum(Exposure, na.rm = TRUE)),
                        by = c(sum.by.base, 'yearmonth')
    ][hyads > 0]
    cat(sprintf("%.1fs  |  %s lignes\n",
                (proc.time() - t1)["elapsed"],
                format(nrow(exposures), big.mark = ",")))
  }
  
  rm(joined); gc(verbose = FALSE)
  tick("Agrégation terminée", t0)
  
  # ── 6. Écriture ───────────────────────────────────────────────────────────
  t0 <- step(5L, N_STEPS, "Écriture")
  
  if (is.null(exp_dir)) {
    exp_dir <- file.path(getwd(), 'exposure')
    message('No exp_dir provided. Defaulting to ', exp_dir)
  }
  dir.create(exp_dir, recursive = TRUE, showWarnings = FALSE)
  
  if (time.agg == 'year') {
    file.yr <- file.path(exp_dir, paste0(link.to, file.by, year.E, '.fst'))
    t1 <- proc.time()
    cat(sprintf("  write.fst -> %s ... ", basename(file.yr)))
    if (nrow(exposures) > 0L) write.fst(exposures, path = file.yr)
    cat(sprintf("%.1fs\n", (proc.time() - t1)["elapsed"]))
    tick(sprintf("%s lignes écrites", format(nrow(exposures), big.mark = ",")), t0)
    
    elapsed_total <- round((proc.time() - t_start)["elapsed"], 1)
    message(sprintf("\n✓ Terminé en %.1fs (%.1f min)  ->  %s",
                    elapsed_total, elapsed_total / 60, file.yr))
    return(exposures[])
    
  } else {
    monthly.filelist <- character(0)
    yms <- sort(unique(exposures$yearmonth))
    pb  <- txtProgressBar(min = 0, max = length(yms), style = 3, width = 55)
    for (i in seq_along(yms)) {
      ym        <- yms[i]
      month_exp <- exposures[yearmonth == ym]
      if (nrow(month_exp) == 0L) next
      file.mo <- file.path(exp_dir, paste0(link.to, file.by, ym, '.fst'))
      write.fst(month_exp, path = file.mo)
      monthly.filelist <- c(monthly.filelist, file.mo)
      setTxtProgressBar(pb, i)
    }
    close(pb); cat("\n")
    tick(sprintf("%d fichiers mensuels écrits", length(monthly.filelist)), t0)
    
    elapsed_total <- round((proc.time() - t_start)["elapsed"], 1)
    message(sprintf("\n✓ Terminé en %.1fs (%.1f min)", elapsed_total, elapsed_total / 60))
    
    if (return.monthly.data) return(exposures[])
    else                     return(monthly.filelist)
  }
}