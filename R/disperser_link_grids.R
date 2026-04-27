#' @export disperser_link_grids
disperser_link_grids <- function(month_YYYYMM = NULL,
                                 start.date = NULL,
                                 end.date = NULL,
                                 unit,
                                 duration.run.hours = 20,
                                 pbl.height = NULL,
                                 res.link. = 12000,
                                 overwrite = FALSE,
                                 pbl. = TRUE,
                                 crop.usa = FALSE,
                                 return.linked.data. = TRUE) {
  
  # Vérifier les arguments
  if (missing(unit) || is.null(unit)) {
    stop("L'argument 'unit' est requis")
  }
  
  unitID <- unit$ID
  
  # Validation des dates
  if (is.null(start.date) && is.null(end.date) && is.null(month_YYYYMM)) {
    stop("Define either a start.date and an end.date OR a month_YYYYMM")
  }
  
  if (dim(unit)[1] > 1) {
    stop("Please supply a single unit (not multiple)")
  }
  
  # Gestion des dates - CORRECTION ICI
  if (!is.null(month_YYYYMM)) {
    # S'assurer que month_YYYYMM est un seul mois (pas un vecteur)
    if (length(month_YYYYMM) > 1) {
      warning("month_YYYYMM doit être un seul mois. Utilisation du premier élément.")
      month_YYYYMM <- month_YYYYMM[1]
    }
    
    # Convertir en caractère et valider le format
    month_YYYYMM <- as.character(month_YYYYMM)
    
    if (nchar(month_YYYYMM) != 6) {
      stop("month_YYYYMM doit être au format YYYYMM (ex: 200511)")
    }
    
    start.date <- as.Date(paste0(
      substr(month_YYYYMM, 1, 4), "-",
      substr(month_YYYYMM, 5, 6), "-01"
    ))
    
    # Calculer la fin du mois
    end.date <- as.Date(paste0(
      substr(month_YYYYMM, 1, 4), "-",
      substr(month_YYYYMM, 5, 6), "-",
      days_in_month(start.date)
    ))
  } else if (!is.null(start.date) && !is.null(end.date)) {
    # S'assurer que ce sont des dates valides
    start.date <- as.Date(start.date)
    end.date <- as.Date(end.date)
    
    # Créer month_YYYYMM à partir des dates
    month_YYYYMM <- format(start.date, "%Y%m")
  } else {
    stop("Dates insuffisantes pour procéder")
  }
  
  # S'assurer que month_YYYYMM est un caractère
  month_YYYYMM <- as.character(month_YYYYMM)
  
  ## name the eventual output file
  output_file <- file.path(
    links_dir,
    paste0("gridlinks_", unit$ID, "_", 
           format(start.date, "%Y-%m-%d"), "_",
           format(end.date, "%Y-%m-%d"), ".fst")
  )
  
  ## Run the linkages
  if (!file.exists(output_file) || overwrite) {
    
    ## identify dates for hyspdisp averages and dates for files to read in
    vec_dates <- as.character(
      seq.Date(
        as.Date(start.date),
        as.Date(end.date),
        by = '1 day'
      )
    )
    
    vec_filedates <- seq.Date(
      from = as.Date(start.date) - ceiling(duration.run.hours / 24),
      to = as.Date(end.date),
      by = '1 day'
    )
    
    ## list the files
    pattern.file <- paste0(
      '_',
      gsub('[*]', '[*]', unit$ID),
      '_(',
      paste(format(vec_filedates, "%Y-%m-%d"), collapse = '|'),
      ').*\\.fst$'
    )
    
    # Créer les chemins de répertoire
    hysp_dir.path <- file.path(
      hysp_dir,
      unique(paste(
        year(vec_filedates),
        formatC(month(vec_filedates), width = 2, flag = '0'),
        sep = '/'
      ))
    )
    
    # Lister les fichiers
    files.read <- character(0)
    for (dir.path in hysp_dir.path) {
      if (dir.exists(dir.path)) {
        files.read <- c(files.read, 
                        list.files(path = dir.path,
                                   pattern = pattern.file,
                                   recursive = FALSE,
                                   full.names = TRUE))
      }
    }
    
    if (length(files.read) == 0) {
      warning(paste("Aucun fichier trouvé pour l'unité", unitID, 
                    "entre", start.date, "et", end.date))
      return(data.table(x = numeric(), y = numeric(), N = numeric(), 
                        month = month_YYYYMM, ID = unitID))
    }
    
    ## read in the files
    l <- lapply(files.read, function(f) {
      tryCatch({
        read.fst(f, as.data.table = TRUE)
      }, error = function(e) {
        warning(paste("Erreur lecture fichier", f, ":", e$message))
        NULL
      })
    })
    
    # Filtrer les NULL
    l <- l[!sapply(l, is.null)]
    
    if (length(l) == 0) {
      warning(paste("Aucune donnée valide lue pour l'unité", unitID))
      return(data.table(x = numeric(), y = numeric(), N = numeric(), 
                        month = month_YYYYMM, ID = unitID))
    }
    
    ## Combine all parcels into single data table
    d <- rbindlist(l, fill = TRUE)
    
    if (nrow(d) == 0 || length(d) == 0) {
      warning(paste("Aucune donnée combinée pour l'unité", unitID))
      return(data.table(x = numeric(), y = numeric(), N = numeric(), 
                        month = month_YYYYMM, ID = unitID))
    }
    
    print(paste(Sys.time(), "Files read and combined"))
    
    ## Trim dates & first hour
    # Vérifier et normaliser les colonnes de date
    if (!"Pdate" %in% names(d)) {
      # Essayer de trouver une colonne de date
      date_cols <- grep("date|Date", names(d), value = TRUE, ignore.case = TRUE)
      if (length(date_cols) > 0) {
        setnames(d, date_cols[1], "Pdate")
      } else {
        warning("Colonne de date non trouvée dans les données")
        return(data.table(x = numeric(), y = numeric(), N = numeric(), 
                          month = month_YYYYMM, ID = unitID))
      }
    }
    
    # Convertir Pdate en Date si ce n'est pas déjà le cas
    if (!inherits(d$Pdate, "Date")) {
      d[, Pdate := as.Date(Pdate)]
    }
    
    # Filtrer par dates et heures
    d <- d[Pdate >= as.Date(start.date) & 
             Pdate <= as.Date(end.date) & 
             hour > 1, ]
    
    if (nrow(d) == 0) {
      warning(paste("Aucune donnée après filtrage des dates pour l'unité", unitID))
      return(data.table(x = numeric(), y = numeric(), N = numeric(), 
                        month = month_YYYYMM, ID = unitID))
    }
    
    ## Trim PBL's
    if (pbl.) {
      if (is.null(pbl.height)) {
        warning("pbl.height n'est pas fourni mais pbl. = TRUE - utilisation sans PBL")
        d_trim <- d
      } else {
        # Vérifier si pbl.height est un raster valide
        if (!inherits(pbl.height, c("Raster", "SpatRaster", "SpatRasterDataset"))) {
          warning("pbl.height n'est pas un objet raster valide - utilisation sans PBL")
          d_trim <- d
        } else {
          tryCatch({
            #Check if extent matches the hpbl raster
            d_xmin <- min(d$lon, na.rm = TRUE)
            e_xmin <- terra::ext(pbl.height)[1]
            
            if (d_xmin < e_xmin - 5) {
              pbl.height <- terra::rotate(pbl.height)
            }
            
            d_trim <- trim_pbl(d, rasterin = pbl.height)
            print(paste(Sys.time(), "PBLs trimmed"))
          }, error = function(e) {
            warning(paste("Erreur lors du trim PBL:", e$message))
            d_trim <- d
          })
        }
      }
    } else {
      d_trim <- d
    }
    
    if (nrow(d_trim) == 0) {
      warning(paste("Aucune donnée après trim PBL pour l'unité", unitID))
      return(data.table(x = numeric(), y = numeric(), N = numeric(), 
                        month = month_YYYYMM, ID = unitID))
    }
    
    ## Link to grid
    p4s <- "+proj=aea +lat_1=20 +lat_2=60 +lat_0=40 +lon_0=-96 +x_0=0 +y_0=0 +ellps=GRS80 +datum=NAD83 +units=m"
    
    tryCatch({
      disp_df_link <- link_to(d = d_trim,
                              link.to = 'grids',
                              p4string = p4s,
                              rasterin = if (pbl.) pbl.height else NULL,
                              res.link. = res.link.,
                              pbl. = pbl.,
                              crop.usa = crop.usa)
      
      print(paste(Sys.time(), "Grids linked"))
      
      if (nrow(disp_df_link) == 0) {
        out <- data.table(x = numeric(), y = numeric(), N = numeric())
      } else {
        out <- disp_df_link
      }
    }, error = function(e) {
      warning(paste("Erreur lors du link_to:", e$message))
      out <- data.table(x = numeric(), y = numeric(), N = numeric())
    })
    
    out$month <- month_YYYYMM
    out$ID <- unitID
    
    if (nrow(out) > 0 && any(!is.na(out$N))) {
      ## write to file
      write.fst(out, output_file)
      print(paste(Sys.time(), "Linked grids and saved to", output_file))
    }
  } else {
    print(paste("File", output_file, "already exists! Use overwrite = TRUE to over write"))
    if (return.linked.data.) {
      out <- read.fst(output_file, as.data.table = TRUE)
    }
  }
  
  if (!exists("out")) {
    out <- data.table(x = numeric(), y = numeric(), N = numeric())
  }
  
  if (!return.linked.data.) {
    out <- data.table(x = numeric(), y = numeric(), N = numeric())
  }
  
  out$month <- month_YYYYMM
  out$ID <- unitID
  
  # Supprimer les colonnes V1 si elles existent
  if ("V1" %in% names(out)) {
    out[, V1 := NULL]
  }
  
  return(out)
}
