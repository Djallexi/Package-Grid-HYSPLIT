


# ── Cache en mémoire ──────────────────────────────────────────────────────────
# path → list(mtime, obs)
.obs_cache <- new.env(hash = TRUE, parent = emptyenv())


# ── Historique des snapshots (pour ETA par vélocité) ─────────────────────────
# Chaque entrée : list(time = POSIXct, files = int, obs = int)
.snap_history <- new.env(hash = TRUE, parent = emptyenv())


# ── Helpers bas niveau ────────────────────────────────────────────────────────

#' find système — 5-10× plus rapide que list.files(recursive=TRUE)
.find_files <- function(dir, extensions = c("fst", "rds", "csv")) {
  ext_pattern <- paste(paste0("*.", extensions), collapse = " -o -name ")
  cmd <- sprintf(
    "find %s \\( -name %s \\) -type f 2>/dev/null",
    shQuote(path.expand(dir)), ext_pattern
  )
  out <- system(cmd, intern = TRUE, ignore.stderr = TRUE)
  out[nzchar(out)]
}

#' Compte lignes CSV par chunks binaires (sans tout charger en RAM)
.csv_nrow <- function(path) {
  con <- file(path, open = "rb")
  on.exit(close(con))
  n <- 0L
  repeat {
    chunk <- readBin(con, raw(), n = 65536L)
    if (!length(chunk)) break
    n <- n + sum(chunk == as.raw(0x0a))
  }
  max(n - 1L, 0L)
}

#' Lecture obs d'un fichier avec cache (invalidé si mtime change)
.obs_one <- function(f, mtime) {
  cached <- .obs_cache[[f]]
  if (!is.null(cached) && cached$mtime == mtime) return(cached$obs)
  obs <- tryCatch(
    switch(tools::file_ext(f),
           fst = as.integer(fst::metadata_fst(f)$nrOfRows),
           rds = nrow(readRDS(f)),
           csv = .csv_nrow(f),
           0L
    ),
    error = function(e) 0L
  )
  .obs_cache[[f]] <- list(mtime = mtime, obs = obs)
  obs
}

#' Compte fichiers + obs — séquentiel (n_cores=1 pour ne pas
#' concurrencer le pipeline principal)
.count_observations <- function(files) {
  if (!length(files)) return(list(files = 0L, obs = 0L))
  info   <- file.info(files)          # un seul appel système
  mtimes <- info$mtime
  valid  <- !is.na(mtimes)
  files  <- files[valid]
  mtimes <- mtimes[valid]
  obs <- vapply(seq_along(files),
                function(i) .obs_one(files[i], mtimes[i]),
                integer(1L))
  list(files = length(files), obs = sum(obs, na.rm = TRUE))
}

#' Filtre les fichiers >= since
.filter_since <- function(files, mtimes, since) {
  if (is.null(since) || !length(files)) return(files)
  files[!is.na(mtimes) & mtimes >= since]
}

#' Parse le progress.log
.read_log <- function(log_file, keys) {
  empty <- setNames(vector("list", length(keys)), keys)
  if (!file.exists(log_file)) return(empty)
  log <- readLines(log_file, warn = FALSE)
  lapply(empty, function(.) NULL)   # init
  lapply(setNames(keys, keys), function(k) {
    line <- grep(paste0("^", k, ":"), log, value = TRUE)
    if (!length(line)) return(NULL)
    sub(paste0(k, ": "), "", line[[1L]])
  })
}


# ── ETA par vélocité glissante ────────────────────────────────────────────────

#' Enregistre un snapshot horodaté
.push_snapshot <- function(session_key, files_done, obs_done) {
  hist <- if (exists(session_key, envir = .snap_history, inherits = FALSE))
    get(session_key, envir = .snap_history) else list()
  hist[[length(hist) + 1L]] <- list(
    time  = Sys.time(),
    files = files_done,
    obs   = obs_done
  )
  # Garder uniquement les 20 derniers points
  if (length(hist) > 20L) hist <- tail(hist, 20L)
  assign(session_key, hist, envir = .snap_history)
}

#' Calcule la vélocité (fichiers/min, obs/min) sur les N derniers snapshots
#' et retourne l'ETA en minutes.
#'
#' Méthode : régression linéaire sur l'historique glissant.
#' Plus robuste qu'une simple différence t-1 / t (moins sensible aux pauses).
#'
#' @param session_key  clé dans .snap_history
#' @param total        nombre total attendu
#' @param window       nombre de points à utiliser (défaut : 10)
#' @return list(rate_files, rate_obs, eta_min, eta_obs_total)
.compute_eta <- function(session_key, total, window = 10L) {
  na_res <- list(rate_files = NA_real_, rate_obs = NA_real_,
                 eta_min = NA_real_, eta_obs_total = NA_real_)
  
  if (!exists(session_key, envir = .snap_history, inherits = FALSE)) return(na_res)
  hist <- get(session_key, envir = .snap_history)
  n    <- length(hist)
  if (n < 2L) return(na_res)
  
  pts  <- tail(hist, min(n, window))
  t0   <- pts[[1L]]$time
  mins <- vapply(pts, function(p) as.numeric(difftime(p$time, t0, units = "mins")), double(1L))
  fls  <- vapply(pts, function(p) p$files, integer(1L))
  obs  <- vapply(pts, function(p) p$obs,   integer(1L))
  
  # Régression linéaire files ~ time et obs ~ time
  if (diff(range(mins)) < 1e-6) return(na_res)   # pas assez de temps écoulé
  
  rate_files <- if (length(unique(fls)) > 1L)
    coef(lm(fls ~ mins))[["mins"]] else NA_real_
  rate_obs   <- if (length(unique(obs)) > 1L)
    coef(lm(obs ~ mins))[["mins"]] else NA_real_
  
  last       <- pts[[length(pts)]]
  remaining  <- if (!is.null(total)) total - last$files else NA_real_
  
  eta_min <- if (!is.null(total) && !is.na(rate_files) && rate_files > 0)
    remaining / rate_files else NA_real_
  
  eta_obs_total <- if (!is.na(rate_obs) && !is.na(rate_files) && rate_files > 0 &&
                       !is.null(total) && last$obs > 0)
    round(last$obs / last$files * total) else NA_real_
  
  list(rate_files    = rate_files,
       rate_obs      = rate_obs,
       eta_min       = eta_min,
       eta_obs_total = eta_obs_total)
}


# ── Barre de progression ──────────────────────────────────────────────────────

.print_bar <- function(counts, total, started_at = NULL,
                       label = "Progress", session_key = "default") {
  done     <- counts$files
  obs_done <- counts$obs
  
  # Enregistrer ce snapshot pour la vélocité
  .push_snapshot(session_key, done, obs_done)
  eta      <- .compute_eta(session_key, total)
  
  pct     <- if (!is.null(total) && total > 0L) round(done / total * 100, 1) else NA_real_
  bar_len <- 40L
  filled  <- if (!is.na(pct)) min(round(pct / 100 * bar_len), bar_len) else 0L
  bar     <- paste0("[", strrep("\u2588", filled), strrep("\u2591", bar_len - filled), "]")
  
  sep <- "══════════════════════════════════════════════"
  cat(sep, "\n", sep = "")
  cat(sprintf(" %s\n", label))
  cat(sep, "\n", sep = "")
  cat(sprintf(" Files   : %s / %s\n",
              format(done,  big.mark = ","),
              if (!is.null(total)) format(total, big.mark = ",") else "?"))
  cat(sprintf(" Obs     : %s rows\n",
              if (!is.na(obs_done)) format(obs_done, big.mark = ",") else "?"))
  cat(sprintf(" Progress: %s %s%%\n", bar, if (is.na(pct)) "?" else pct))
  
  # ── Timing via vélocité glissante ──────────────────────────────────────────
  if (!is.null(started_at) && !is.na(pct) && pct > 0) {
    elapsed_min <- as.numeric(difftime(Sys.time(), started_at, units = "mins"))
    cat(sprintf(" Elapsed : %.1f min (%.1f h)\n", elapsed_min, elapsed_min / 60))
  }
  
  if (pct >= 100) {
    cat(" Status  : \u2705 COMPLETED\n")
  } else {
    # Vélocité instantanée (régression glissante)
    if (!is.na(eta$rate_files) && eta$rate_files > 0) {
      cat(sprintf(" Speed   : %s files/min",
                  format(round(eta$rate_files), big.mark = ",")))
      if (!is.na(eta$rate_obs) && eta$rate_obs > 0)
        cat(sprintf("  |  %s rows/min", format(round(eta$rate_obs), big.mark = ",")))
      cat("\n")
    }
    
    # ETA
    if (!is.na(eta$eta_min)) {
      cat(sprintf(" ETA     : %.0f min (≈ %.1f h)\n", eta$eta_min, eta$eta_min / 60))
    } else if (!is.null(started_at) && !is.na(pct) && pct > 0) {
      # Fallback : méthode linéaire simple si pas encore assez de points
      elapsed_min <- as.numeric(difftime(Sys.time(), started_at, units = "mins"))
      eta_simple  <- elapsed_min / (pct / 100) * (1 - pct / 100)
      cat(sprintf(" ETA     : ~%.0f min (≈ %.1f h)  [estimation initiale]\n",
                  eta_simple, eta_simple / 60))
    }
    
    # Total obs estimé
    if (!is.na(eta$eta_obs_total))
      cat(sprintf(" Obs est.: ~%s rows total\n",
                  format(eta$eta_obs_total, big.mark = ",")))
  }
  
  cat(sep, "\n", sep = "")
  invisible(pct)
}


# ── Boucle watch ─────────────────────────────────────────────────────────────

.watch_loop <- function(count_fn, bar_fn, total, interval, done_msg) {
  cat("\n\U0001f441  Watching progress (Ctrl+C to stop)...\n\n")
  repeat {
    counts <- count_fn()
    cat(sprintf("\n[%s] ", format(Sys.time(), "%H:%M:%S")))
    pct <- bar_fn(counts)
    if (!is.null(total) && !is.na(pct) && pct >= 100) {
      cat("\n\U0001f389 ", done_msg, "\n", sep = "")
      break
    }
    Sys.sleep(interval)
  }
  invisible(counts)
}


# ── Fonctions publiques ───────────────────────────────────────────────────────

#' Surveille la progression des simulations HYSPLIT
#'
#' @param hysp_dir   répertoire des sorties HYSPLIT
#' @param output_dir répertoire du progress.log
#' @param total      nombre total de simulations (auto si NULL)
#' @param watch      TRUE = rafraîchit en continu
#' @param interval   secondes entre rafraîchissements
check_progress_Hysplit <- function(
    hysp_dir   = "/var/tmp/hysp_data",
    output_dir = "~/work/main/output/linked_grids",
    total      = NULL,
    watch      = FALSE,
    interval   = 30L
) {
  meta <- .read_log(file.path(output_dir, "progress.log"), c("Total", "Started"))
  if (is.null(total) && !is.null(meta$Total)) total <- as.numeric(meta$Total)
  started_at <- if (!is.null(meta$Started)) as.POSIXct(meta$Started) else NULL
  
  count_fn <- function() {
    files  <- .find_files(hysp_dir, c("fst", "rds", "csv"))
    info   <- file.info(files)
    .count_observations(.filter_since(files, info$mtime, started_at))
  }
  bar_fn <- function(counts)
    .print_bar(counts, total, started_at,
               label = "HYSPLIT Simulations", session_key = "hysplit")
  
  if (!watch) {
    cat("\n"); counts <- count_fn(); bar_fn(counts); return(invisible(counts))
  }
  .watch_loop(count_fn, bar_fn, total, interval, "All HYSPLIT simulations completed!")
}


#' Surveille la progression du Grid Linking
#'
#' @param output_dir  répertoire des fichiers liés + progress.log
#' @param year        année filtrée dans les noms de fichiers
#' @param total_units nombre total d'unités (auto si NULL)
#' @param watch       TRUE = rafraîchit en continu
#' @param interval    secondes entre rafraîchissements
check_progress_GridLinking <- function(
    output_dir  = "~/work/main/output/linked_grids",
    year        = 2000L,
    total_units = NULL,
    watch       = FALSE,
    interval    = 30L
) {
  meta <- .read_log(
    file.path(output_dir, "progress.log"),
    c("GridTotal", "Grid linking started")
  )
  if (is.null(total_units) && !is.null(meta$GridTotal))
    total_units <- as.numeric(meta$GridTotal)
  started_at <- if (!is.null(meta[["Grid linking started"]]))
    as.POSIXct(meta[["Grid linking started"]]) else NULL
  
  count_fn <- function() {
    files  <- .find_files(output_dir, c("fst", "rds"))
    files  <- grep(paste0("linked_.*", year), files, value = TRUE)
    info   <- file.info(files)
    .count_observations(.filter_since(files, info$mtime, started_at))
  }
  bar_fn <- function(counts)
    .print_bar(counts, total_units, started_at,
               label = "Grid Linking Progress", session_key = "gridlink")
  
  if (!watch) {
    cat("\n"); counts <- count_fn(); bar_fn(counts); return(invisible(counts))
  }
  .watch_loop(count_fn, bar_fn, total_units, interval, "Grid Linking completed!")
}