# ============================================================
# HYSPLIT SIMULATIONS -> LINKED GRIDS -> EXPOSURE GRIDS
# WGS84 native — 2 output files :
#   linked_grids_{yr}_native_withID.csv.gz
#   exposure_monthly_{yr}_unit.csv.gz
# ============================================================


# ============================================================
# CONFIGURATION
# ============================================================
TARGET_YEAR <- 2022    # ← changer uniquement cette valeur

CONFIG <- list(
  # Github tokens
  github_pat     = "",
  
  # Data
  units_file     = "~/work/main/input/Plant Units/units_coal_SO2_finalv3.csv",
  pblheight_file = "~/work/main/input/hpbl/hpbl.mon.mean.nc",
  
  # Simulation (dérivés de TARGET_YEAR)
  year           = TARGET_YEAR,
  startday       = paste0(TARGET_YEAR, "-01-01"),
  endday         = paste0(TARGET_YEAR, "-12-31"),
  start_hours    = c(0, 12),
  duration       = 120,
  npart          = 100,
  numberofplants = 50,
  
  # Linking
  yearmons_start_month = "01",
  yearmons_end_month   = "12",
  duration_run_hours   = 120,
  
  # Exposure (dérivé de TARGET_YEAR)
  year_exposure = TARGET_YEAR,
  pollutant     = "SO2_month",
  
  # Répertoires
  meteo_dir  = "/var/tmp/meteo_data",
  proc_dir   = "/var/tmp/proc_data",
  hysp_dir   = "/var/tmp/hysp_data",
  output_dir = "~/work/main/output/linked_grids",
  rdata_dir  = "~/work/main/output/linked_grids/rdata",
  exp_dir    = "~/work/main/output/exposure",
  log_dir    = "~/work/main/output/logs",
  
  # Parallelism
  cores = parallel::detectCores() - 1,
  
  # Options
  overwrite   = TRUE,
  subset_days = NULL   # NULL = every day / example : c(1,15) = days 1 and 15 only
)

Sys.setenv(GITHUB_PAT = CONFIG$github_pat)


# ============================================================
# HELPERS
# ============================================================

# ── Logger ────────────────────────────────────────────────────────────────────
.log_con <- NULL

log_open <- function(log_dir, prefix = "pipeline") {
  dir.create(log_dir, recursive = TRUE, showWarnings = FALSE)
  path     <- file.path(log_dir, paste0(prefix, "_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".log"))
  .log_con <<- file(path, open = "wt")
  log_msg(paste("Log opened:", path))
  invisible(path)
}

log_msg <- function(...) {
  msg <- paste0("[", format(Sys.time(), "%H:%M:%S"), "] ", paste(...))
  message(msg)
  if (!is.null(.log_con)) writeLines(msg, .log_con)
}

log_close <- function() {
  if (!is.null(.log_con)) { close(.log_con); .log_con <<- NULL }
}

# ── Timer ─────────────────────────────────────────────────────────────────────
timer <- new.env()
timer$start <- function(name) {
  timer[[name]] <- Sys.time()
  log_msg("⏱  Starting:", name)
}
timer$stop <- function(name) {
  timer[[paste0(name, "_end")]] <- Sys.time()
  elapsed <- difftime(timer[[paste0(name, "_end")]], timer[[name]], units = "mins")
  log_msg("✓  Done:", name, "→", round(elapsed, 2), "min")
  return(elapsed)
}

# ── Section header ────────────────────────────────────────────────────────────
section <- function(title) {
  width   <- 55
  padding <- max(0, width - nchar(title) - 4)
  log_msg("\n╔", paste(rep("═", width), collapse = ""), "╗")
  log_msg("║  ", title, paste(rep(" ", padding), collapse = ""), "  ║")
  log_msg("╚", paste(rep("═", width), collapse = ""), "╝\n")
}


# ============================================================
section("HYSPLIT → LINKED GRIDS PIPELINE (WGS84 native)")
# ============================================================
log_open(CONFIG$log_dir, prefix = paste0("pipeline_", CONFIG$year))
log_msg("Started  :", as.character(Sys.time()))
log_msg("Cores    :", CONFIG$cores)
log_msg("Period   :", CONFIG$startday, "→", CONFIG$endday)
log_msg("Year     :", CONFIG$year)
log_msg("CRS      : WGS84 native (res = 0.1°)")
log_msg("Outputs  : 2 fichiers — native_withID.csv.gz + exposure_monthly_unit.csv.gz")

pipeline_start <- Sys.time()


# ============================================================
section("1. LOAD LIBRARIES")
# ============================================================
timer$start("Libraries")

install.packages(c("gridExtra", "ggmap", "ggrepel", "fst", "viridis", "dplyr", "ggspatial"))

suppressPackageStartupMessages({
  library(parallelly)
  library(ncdf4)
  library(data.table)
  library(tidyverse)
  library(parallel)
  library(sf)
  library(terra)
  library(raster)
  library(sp)
  library(viridis)
  library(ggplot2)
  library(scales)
  library(gridExtra)
  library(ggrepel)
  library(fst)
  library(dplyr)
  library(broom)
  library(lubridate)
  library(arrow)
  library(UpdatedDisperseR)
})
timer$stop("Libraries")


# ============================================================
section("2. SOURCE FUNCTIONS")
# ============================================================
timer$start("Functions")
#source("~/work/Execute All Functions.R")
timer$stop("Functions")


# ============================================================
section("3. SETUP DIRECTORIES")
# ============================================================
timer$start("Directories")

create_dirs("~/work")

dirs <- list(
  meteo  = CONFIG$meteo_dir,
  proc   = CONFIG$proc_dir,
  hysp   = CONFIG$hysp_dir,
  output = CONFIG$output_dir,
  rdata  = CONFIG$rdata_dir,
  exp    = CONFIG$exp_dir,
  log    = CONFIG$log_dir
)
invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))

assign("meteo_dir", dirs$meteo, envir = .GlobalEnv)
assign("proc_dir",  dirs$proc,  envir = .GlobalEnv)
assign("hysp_dir",  dirs$hysp,  envir = .GlobalEnv)

timer$stop("Directories")


# ============================================================
section("4. METEOROLOGICAL DATA")
# ============================================================
timer$start("Meteo")

meteo_start <- format(floor_date(as.Date(CONFIG$startday) %m-% months(1), "month"), "%Y-%m-%d")
meteo_end   <- format(floor_date(as.Date(CONFIG$endday)   %m+% months(1), "month"), "%Y-%m-%d")

meteo_start_year  <- format(as.Date(meteo_start), "%Y")
meteo_start_month <- format(as.Date(meteo_start), "%m")
meteo_end_year    <- format(as.Date(meteo_end),   "%Y")
meteo_end_month   <- format(as.Date(meteo_end),   "%m")

log_msg("Meteo window:", meteo_start, "→", meteo_end)

met_files <- list.files(meteo_dir,
                        pattern    = paste0("^RP(", meteo_start_year, "|", meteo_end_year, ")"),
                        full.names = FALSE)

n_months_expected <-
  (as.numeric(meteo_end_year)  - as.numeric(meteo_start_year)) * 12 +
  (as.numeric(meteo_end_month) - as.numeric(meteo_start_month)) + 1

log_msg("Files found    :", length(met_files))
log_msg("Months expected:", n_months_expected)

if (length(met_files) < n_months_expected) {
  log_msg("Downloading missing files...")
  get_data(data        = "metfiles",
           start.year  = meteo_start_year,
           start.month = meteo_start_month,
           end.year    = meteo_end_year,
           end.month   = meteo_end_month)
} else {
  log_msg("All files already available")
}
timer$stop("Meteo")


# ============================================================
section("5. PBL HEIGHT DATA")
# ============================================================
timer$start("PBL Height")

if (!file.exists(CONFIG$pblheight_file))
  stop("PBL height file not found: ", CONFIG$pblheight_file)

Sys.setenv(TZ = "UTC")
pblheight <- terra::rast(CONFIG$pblheight_file, subds = "hpbl")
terra::crs(pblheight) <- "+proj=longlat +datum=WGS84"
assign("pblheight", pblheight, envir = .GlobalEnv)
log_msg("Layers:", terra::nlyr(pblheight))

timer$stop("PBL Height")


# ============================================================
section("6. UNITS DATA")
# ============================================================
timer$start("Units")

units <- read.csv(CONFIG$units_file)

rename_map <- c(uid_plant = "ID", yr = "year", latitude = "Latitude", longitude = "Longitude")
for (old in names(rename_map)) {
  new <- rename_map[old]
  if (old %in% names(units)) names(units)[names(units) == old] <- new
}

units <- units %>%
  dplyr::mutate(
    Height = dplyr::case_when(
      totcap >= 0.5                  ~ 275,
      totcap >= 0.21 & totcap < 0.5 ~ 220,
      totcap < 0.21                  ~ 150,
      TRUE                           ~ NA_real_
    ),
    uID = ID
  )

unitsrunTopAll <- units %>%
  dplyr::filter(year == CONFIG$year, Type.of.Coal %in% c("COAL", "LIGN")) %>%
  dplyr::group_by(year) %>%
  dplyr::slice_max(SO2_total_kt, n = CONFIG$numberofplants, with_ties = FALSE) %>%
  dplyr::ungroup()

unitsrun <- data.table::as.data.table(unitsrunTopAll)

# Construire units_monthly (nécessaire pour l'exposition)
unitsdt       <- data.table::as.data.table(units)
units_monthly <- unitsdt[year == CONFIG$year & Type.of.Coal %in% c("COAL", "LIGN"),
                         .SD[rep(1:.N, each = 12)]]
units_monthly[, `:=`(
  uID       = as.character(ID),
  month     = rep(1:12, times = nrow(units_monthly) / 12),
  SO2_month = SO2_total_kt / 12
)]

log_msg("Units loaded  :", nrow(unitsrun))
log_msg("SO2 range     :", round(min(unitsrun$SO2_total_kt), 2), "–",
        round(max(unitsrun$SO2_total_kt), 2))
log_msg("units_monthly :", nrow(units_monthly), "lignes")
timer$stop("Units")


# ============================================================
section("7. INPUT PARAMETERS")
# ============================================================
timer$start("Inputs")

input_refs <- define_inputs(
  units       = unitsrun,
  startday    = CONFIG$startday,
  endday      = CONFIG$endday,
  start.hours = CONFIG$start_hours,
  duration    = CONFIG$duration
)

input_refs_subset <- if (!is.null(CONFIG$subset_days)) {
  input_refs[
    as.integer(format(as.Date(input_refs$start_day), "%d")) %in% CONFIG$subset_days &
      input_refs$start_hour == 0
  ]
} else {
  input_refs
}

log_msg("Total refs  :", nrow(input_refs))
log_msg("Subset refs :", nrow(input_refs_subset))
timer$stop("Inputs")


# ============================================================
section("8. HYSPLIT SIMULATIONS")
# ============================================================
timer$start("HYSPLIT")

hysp_raw <- run_disperser_parallel(
  input.refs         = input_refs_subset,
  pbl.height         = pblheight,
  species            = "so2",
  proc_dir           = dirs$proc,
  overwrite          = CONFIG$overwrite,
  npart              = CONFIG$npart,
  keep.hysplit.files = FALSE,
  mc.cores           = CONFIG$cores
)

hysplit_time <- timer$stop("HYSPLIT")


# ============================================================
section("9. LINK TO GRIDS")
# ============================================================
timer$start("Grid Linking")

yearmons <- get_yearmon(
  start.year  = as.character(CONFIG$year),
  start.month = CONFIG$yearmons_start_month,
  end.year    = as.character(CONFIG$year),
  end.month   = CONFIG$yearmons_end_month
)
log_msg("Months :", paste(yearmons, collapse = ", "))

linked_grids <- link_all_units(
  units.run          = unitsrun[unitsrun$year == CONFIG$year, ],
  link.to            = "grids",
  mc.cores           = CONFIG$cores,
  year.mons          = yearmons,
  pbl_trim           = FALSE,
  duration.run.hours = CONFIG$duration_run_hours,
  overwrite          = CONFIG$overwrite
)

if (is.null(linked_grids) || nrow(linked_grids) == 0)
  stop("linked_grids est vide — vérifier les simulations HYSPLIT.")

# Fix floating point noise
linked_grids <- data.table::as.data.table(linked_grids)
linked_grids[, `:=`(x = round(x, 1), y = round(y, 1))]
linked_grids <- linked_grids[, .(N = sum(N, na.rm = TRUE)),
                             by = .(x, y, ID, month, comb)]

log_msg("Rows    :", format(nrow(linked_grids), big.mark = ","))
log_msg("x range :", round(min(linked_grids$x), 3), "→", round(max(linked_grids$x), 3), "(lon °)")
log_msg("y range :", round(min(linked_grids$y), 3), "→", round(max(linked_grids$y), 3), "(lat °)")

linking_time <- timer$stop("Grid Linking")


# ============================================================
section("9b. COMBINE MONTHLY LINKS")
# ============================================================
timer$start("Combine Monthly Links")

combined_gridlinks <- combine_monthly_links(
  month_YYYYMMs = yearmons,
  link.to       = "grids",
  filename      = "hyads_vig_unwgted_grids.RData"
)

rda_path <- file.path(dirs$rdata, "hyads_vig_unwgted_grids.RData")
save(combined_gridlinks, file = rda_path)
log_msg("✓ RData :", rda_path)

timer$stop("Combine Monthly Links")


# ============================================================
section("10. SAVE LINKED GRIDS (native_withID uniquement)")
# ============================================================
timer$start("Saving Linked Grids")

yr          <- CONFIG$year
lg_out_path <- file.path(dirs$output, paste0("linked_grids_", yr, "_native_withID.csv.gz"))

data.table::fwrite(linked_grids, lg_out_path, compress = "gzip")
log_msg("✓ Linked grids :", lg_out_path,
        sprintf("(%.1f MB)", file.size(lg_out_path) / 1e6))

saving_time <- timer$stop("Saving Linked Grids")


# ============================================================
section("11. CALCULATE & SAVE EXPOSURE (mensuelle par unité)")
# ============================================================
timer$start("Exposure")

yre            <- CONFIG$year_exposure
valid_uids_chr <- unique(units_monthly$uID)

# Filtrer linked_grids sur les uID valides
lg_exp <- linked_grids[ID %in% valid_uids_chr, .(x, y, ID, month, N)]

# Calcul exposition mensuelle
log_msg("Calcul exposition mensuelle...")
t0 <- proc.time()

exp_monthly <- calculate_exposure(
  year.E              = yre,
  year.D              = yre,
  link.to             = "grids",
  pollutant           = CONFIG$pollutant,
  units.mo            = units_monthly,
  linked_grids        = lg_exp,
  exp_dir             = dirs$exp,
  source.agg          = "unit",
  time.agg            = "month",
  return.monthly.data = TRUE
)

if (is.null(exp_monthly) || nrow(exp_monthly) == 0)
  stop("exp_monthly est vide — vérifier linked_grids et units_monthly.")

log_msg(sprintf("  -> mensuel : %s lignes en %.1f sec",
                format(nrow(exp_monthly), big.mark = ","),
                (proc.time() - t0)["elapsed"]))

# Agréger annuel par unité (pour log uniquement)
exp_unit <- data.table::as.data.table(exp_monthly)[,
                                                   .(hyads = sum(hyads, na.rm = TRUE)), by = .(x, y, uID)
][hyads > 0]
log_msg(sprintf("  -> annuel par unité (contrôle) : %s lignes",
                format(nrow(exp_unit), big.mark = ",")))

# Renommer x/y → lon/lat et sauvegarder
data.table::setnames(exp_monthly, c("x", "y"), c("lon", "lat"))

exp_out_path <- file.path(dirs$exp, paste0("exposure_monthly_", yre, "_unit.csv.gz"))
data.table::fwrite(exp_monthly, exp_out_path, compress = "gzip")
log_msg("✓ Exposition mensuelle :", exp_out_path,
        sprintf("(%.1f MB)", file.size(exp_out_path) / 1e6))

exp_time <- timer$stop("Exposure")


# ============================================================
section("PIPELINE SUMMARY")
# ============================================================
total_time <- difftime(Sys.time(), pipeline_start, units = "hours")

log_msg(sprintf("%-22s %s",     "Year:",        CONFIG$year))
log_msg(sprintf("%-22s %s → %s","Period:",      CONFIG$startday, CONFIG$endday))
log_msg(sprintf("%-22s %d",     "Units:",       nrow(unitsrun)))
log_msg(sprintf("%-22s %d",     "Simulations:", nrow(input_refs_subset)))
log_msg(sprintf("%-22s %s",     "Grid cells:",  format(nrow(linked_grids), big.mark = ",")))
log_msg(sprintf("%-22s %s",     "CRS:",         "WGS84 native (res = 0.1°)"))

log_msg("\n─── Timing ────────────────────────────────────────")
fmt <- "  %-28s %.2f min"
log_msg(sprintf(fmt, "Libraries:",       as.numeric(difftime(timer[["Libraries_end"]],             timer[["Libraries"]],             units = "mins"))))
log_msg(sprintf(fmt, "Functions:",       as.numeric(difftime(timer[["Functions_end"]],             timer[["Functions"]],             units = "mins"))))
log_msg(sprintf(fmt, "Directories:",     as.numeric(difftime(timer[["Directories_end"]],           timer[["Directories"]],           units = "mins"))))
log_msg(sprintf(fmt, "Meteo:",           as.numeric(difftime(timer[["Meteo_end"]],                 timer[["Meteo"]],                 units = "mins"))))
log_msg(sprintf(fmt, "PBL Height:",      as.numeric(difftime(timer[["PBL Height_end"]],            timer[["PBL Height"]],            units = "mins"))))
log_msg(sprintf(fmt, "Units:",           as.numeric(difftime(timer[["Units_end"]],                 timer[["Units"]],                 units = "mins"))))
log_msg(sprintf(fmt, "Inputs:",          as.numeric(difftime(timer[["Inputs_end"]],                timer[["Inputs"]],                units = "mins"))))
log_msg(sprintf(fmt, "HYSPLIT:",         as.numeric(hysplit_time)))
log_msg(sprintf(fmt, "Grid Linking:",    as.numeric(linking_time)))
log_msg(sprintf(fmt, "Combine Monthly:", as.numeric(difftime(timer[["Combine Monthly Links_end"]], timer[["Combine Monthly Links"]], units = "mins"))))
log_msg(sprintf(fmt, "Saving LG:",       as.numeric(saving_time)))
log_msg(sprintf(fmt, "Exposure:",        as.numeric(exp_time)))
log_msg("───────────────────────────────────────────────────")
log_msg(sprintf("  %-28s %.2f hours", "TOTAL:", as.numeric(total_time)))
log_msg("───────────────────────────────────────────────────")

log_msg("\n┌── OUTPUTS (lon/lat WGS84, res = 0.1°) ──────────────────────────┐")
log_msg("│  linked_grids/")
log_msg("│    native_withID :", lg_out_path)
log_msg("│  exposure/")
log_msg("│    monthly unit  :", exp_out_path)
log_msg("└─────────────────────────────────────────────────────────────────┘")

log_msg("\nTo load:")
log_msg('  linked <- data.table::fread("', lg_out_path,  '")')
log_msg('  exp    <- data.table::fread("', exp_out_path, '")')

log_msg("\nFinished:", as.character(Sys.time()))
log_close()
