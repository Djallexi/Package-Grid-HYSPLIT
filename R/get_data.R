#' Download meteorological and PBL data with disk space checking
#' @export get_data
#' @export download_file

download_file <- function(url, file, dir) {
  out <- tryCatch({
    download.file(url = url, destfile = file, mode = "wb", method = "auto")
    return(TRUE)
  },
  error = function(cond) {
    message("Download error: ", conditionMessage(cond))
    return(FALSE)
  })
  return(out)
}

#' Check available disk space
#' @param dir_path Path to check
#' @param required_gb Minimum GB required
check_disk_space <- function(dir_path, required_gb = 50) {
  if (.Platform$OS.type == "windows") {
    # Windows
    drive <- substr(normalizePath(dir_path), 1, 2)
    cmd <- paste0('wmic logicaldisk where "DeviceID=\'', drive, '\'" get FreeSpace')
    free_bytes <- tryCatch({
      as.numeric(system(cmd, intern = TRUE)[2])
    }, error = function(e) NA)
    
    if (is.na(free_bytes)) {
      warning("Could not determine disk space on Windows")
      return(invisible(NULL))
    }
    free_gb <- free_bytes / (1024^3)
  } else {
    # Unix/Linux/Mac
    df_output <- tryCatch({
      system(paste("df -k", shQuote(dir_path)), intern = TRUE)
    }, error = function(e) {
      warning("Could not determine disk space")
      return(NULL)
    })
    
    if (is.null(df_output) || length(df_output) < 2) {
      warning("Could not parse disk space information")
      return(invisible(NULL))
    }
    
    avail_line <- df_output[2]
    parts <- strsplit(trimws(avail_line), "\\s+")[[1]]
    
    # Format: Filesystem Size Used Avail Use% Mounted
    # Available is typically the 4th column
    avail_kb <- as.numeric(parts[4])
    free_gb <- avail_kb / (1024^2)
  }
  
  message(sprintf("✓ Available disk space: %.1f GB", free_gb))
  
  if (free_gb < required_gb) {
    stop(sprintf(
      "Insufficient disk space!\n",
      "  Required: ~%d GB\n",
      "  Available: %.1f GB\n",
      "  Location: %s\n",
      "  Please move to a directory with more space or free up disk space.",
      required_gb, free_gb, dir_path
    ))
  }
  
  return(invisible(free_gb))
}

get_data <- function(data,
                     start.year = NULL,
                     start.month = NULL,
                     end.year = NULL,
                     end.month = NULL) {
  
  if (data == "all" || data == "pblheight") {
    
    ### HPBL (Planetary Boundary Layer)
    message("Downloading planetary boundary layer data")
    directory <- hpbl_dir
    
    # Create directory if it doesn't exist
    if (!dir.exists(directory)) {
      dir.create(directory, recursive = TRUE, showWarnings = FALSE)
      message("   Created directory: ", directory)
    }
    
    # Check disk space (need ~1 GB for HPBL)
    check_disk_space(directory, required_gb = 2)
    
    file <- file.path(directory, 'hpbl.mon.mean.nc')
    
    # HTTPS URL from NOAA
    url <- 'https://downloads.psl.noaa.gov/Datasets/ncep.reanalysis/surface/hpbl.mon.mean.nc'
    
    # Remove old file if it exists and is corrupted
    if (file.exists(file)) {
      file_size_mb <- file.size(file) / (1024^2)
      if (file_size_mb < 1) {  # File should be several MB
        message("   Removing corrupted HPBL file (", round(file_size_mb, 2), " MB)...")
        file.remove(file)
      } else {
        message("   HPBL file already exists (", round(file_size_mb, 2), " MB), skipping download")
        
        # Load existing file
        Sys.setenv(TZ = 'UTC')
        tryCatch({
          hpbl_rasterin <- terra::rast(x = file, subds = 'hpbl')
          terra::crs(hpbl_rasterin) <- "+proj=longlat +datum=WGS84"
          assign("pblheight", hpbl_rasterin, envir = .GlobalEnv)
          message("   ✓ pblheight assigned to global environment")
        }, error = function(e) {
          stop("Error reading existing HPBL file: ", conditionMessage(e))
        })
        
        if (data == "pblheight") {
          return(invisible(NULL))
        } else {
          # Continue to metfiles section
        }
      }
    }
    
    # Download if file doesn't exist
    if (!file.exists(file)) {
      message("   Downloading HPBL data from NOAA...")
      success <- download_file(url, file, directory)
      
      if (!success) {
        stop("Failed to download HPBL data. Please check your internet connection or try again later.")
      }
      
      # Verify file was downloaded and has reasonable size
      if (!file.exists(file) || file.size(file) < 1000) {
        stop("Downloaded HPBL file is invalid or too small")
      }
      
      message("   ✓ Downloaded HPBL file (", round(file.size(file)/(1024^2), 2), " MB)")
      
      # Read and prepare the file
      Sys.setenv(TZ = 'UTC')
      
      tryCatch({
        hpbl_rasterin <- terra::rast(x = file, subds = 'hpbl')
        terra::crs(hpbl_rasterin) <- "+proj=longlat +datum=WGS84"
        
        assign("pblheight", hpbl_rasterin, envir = .GlobalEnv)
        message("   ✓ pblheight assigned to global environment")
      }, error = function(e) {
        stop("Error reading HPBL file: ", conditionMessage(e))
      })
    }
    
    if (data == "pblheight") {
      return(invisible(NULL))
    }
  }
  
  if (data == "all" || data == "metfiles") {
    
    ### METFILES (Meteorological data)
    if (is.null(start.year) | is.null(start.month) | 
        is.null(end.year) | is.null(end.month)) {
      stop("Please specify start.year, start.month, end.year, and end.month for metfiles")
    }
    
    message("Downloading meteorological files")
    
    # Create directory if it doesn't exist
    if (!dir.exists(meteo_dir)) {
      dir.create(meteo_dir, recursive = TRUE, showWarnings = FALSE)
      message("   Created directory: ", meteo_dir)
    }
    
    # Calculate how many files we need
    inputdates <- c(
      paste(start.year, start.month, "01", sep = "/"),
      paste(end.year, end.month, "01", sep = "/")
    )
    start_date <- as.Date(inputdates[1])
    end_date <- as.Date(inputdates[2])
    
    vectorfiles <- NULL
    i <- 1
    current_date <- start_date
    
    while (current_date <= end_date) {
      string <- paste0("RP", format(current_date, "%Y%m"), ".gbl")
      vectorfiles[i] <- string
      lubridate::month(current_date) <- lubridate::month(current_date) + 1
      i <- i + 1
    }
    
    # Calculate required space (each file ~120 MB)
    n_files <- length(vectorfiles)
    required_gb <- ceiling(n_files * 120 / 1024) + 5  # Add 5 GB buffer
    
    message(sprintf("   Need to download %d files (~%d GB total)", 
                    n_files, required_gb))
    
    # Check disk space
    check_disk_space(meteo_dir, required_gb = required_gb)
    
    # Filter to only files we don't have
    existing_files <- list.files(meteo_dir)
    metfiles <- vectorfiles[!(vectorfiles %in% existing_files)]
    
    # Also check if existing files are complete (>100 MB)
    for (existing in existing_files[existing_files %in% vectorfiles]) {
      filepath <- file.path(meteo_dir, existing)
      if (file.size(filepath) < 100000000) {  # Less than 100 MB
        message("   Removing incomplete file: ", existing, 
                " (", round(file.size(filepath)/(1024^2), 1), " MB)")
        file.remove(filepath)
        metfiles <- c(metfiles, existing)
      }
    }
    
    if (length(metfiles) > 0) {
      message(sprintf("   Files to download: %d", length(metfiles)))
      message("   Starting downloads...")
      
      # Download with improved function
      get_met_reanalysis_https(files = metfiles, path_met_files = meteo_dir)
      
      message("   ✓ Download process completed")
    } else {
      message("   ✓ All meteorological files already available")
    }
  }
  
  return(invisible(NULL))
}

#' Download meteorological files from NOAA with robust error handling
#' @export get_met_reanalysis_https
get_met_reanalysis_https <- function(files = NULL,
                                     years = NULL,
                                     path_met_files,
                                     max_retries = 3,
                                     timeout = 900) {  # 15 minutes default
  
  # Set longer timeout for large files
  old_timeout <- getOption("timeout")
  options(timeout = timeout)
  on.exit(options(timeout = old_timeout), add = TRUE)
  
  reanalysis_url <- "https://www.ready.noaa.gov/data/archives/reanalysis/"
  
  # Download list of reanalysis met files by name
  if (!is.null(files)) {
    
    successful <- 0
    failed <- 0
    
    for (i in 1:length(files)) {
      url <- paste0(reanalysis_url, files[i])
      destfile <- file.path(path_met_files, files[i])
      
      # Skip if valid file already exists
      if (file.exists(destfile) && file.size(destfile) > 100000000) {
        message(sprintf("   [%d/%d] ✓ %s already exists (%.1f MB), skipping", 
                        i, length(files), files[i], 
                        file.size(destfile)/(1024^2)))
        successful <- successful + 1
        next
      }
      
      # Remove partial downloads
      if (file.exists(destfile)) {
        file.remove(destfile)
      }
      
      message(sprintf("   [%d/%d] Downloading %s...", i, length(files), files[i]))
      
      # Retry loop
      success <- FALSE
      for (attempt in 1:max_retries) {
        if (attempt > 1) {
          message(sprintf("      Retry %d/%d...", attempt, max_retries))
          Sys.sleep(3)  # Wait between retries
        }
        
        success <- tryCatch({
          download.file(
            url = url,
            destfile = destfile,
            method = "libcurl",
            quiet = FALSE,
            mode = "wb",
            cacheOK = FALSE
          )
          
          # Verify download is complete
          if (file.exists(destfile) && file.size(destfile) > 100000000) {
            TRUE
          } else {
            message(sprintf("      Downloaded file too small (%.1f MB), retrying...", 
                            file.size(destfile)/(1024^2)))
            if (file.exists(destfile)) file.remove(destfile)
            FALSE
          }
        }, error = function(e) {
          message("      Error: ", conditionMessage(e))
          if (file.exists(destfile)) file.remove(destfile)
          FALSE
        })
        
        if (success) {
          message(sprintf("      ✓ Success (%.1f MB)", 
                          file.size(destfile)/(1024^2)))
          break
        }
      }
      
      if (success) {
        successful <- successful + 1
      } else {
        failed <- failed + 1
        warning(sprintf("Could not download %s after %d attempts", 
                        files[i], max_retries))
      }
      
      # Progress update every 10 files
      if (i %% 10 == 0) {
        message(sprintf("   Progress: %d/%d files (✓ %d successful, ✗ %d failed)", 
                        i, length(files), successful, failed))
      }
    }
    
    # Final summary
    message(sprintf("\n   Download Summary:"))
    message(sprintf("   ✓ Successful: %d", successful))
    message(sprintf("   ✗ Failed: %d", failed))
    message(sprintf("   Total: %d", length(files)))
  }
  
  # Download one or more years of reanalysis met files
  if (!is.null(years)) {
    all_files <- c()
    for (year in years) {
      for (month in 1:12) {
        filename <- paste0("RP", year, formatC(month, width = 2, format = "d", flag = "0"), ".gbl")
        all_files <- c(all_files, filename)
      }
    }
    
    message(sprintf("Downloading %d files for years: %s", 
                    length(all_files), paste(years, collapse = ", ")))
    
    # Call recursively with files parameter
    get_met_reanalysis_https(files = all_files, 
                             path_met_files = path_met_files,
                             max_retries = max_retries,
                             timeout = timeout)
  }
  
  return(invisible(NULL))
}