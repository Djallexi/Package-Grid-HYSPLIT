

#' @export read_gridlinks_subfun
read_gridlinks_subfun <- function(i, files) {
  d <- read.fst(files[i], as.data.table = TRUE)
  d[, month := as( month, 'character')]
  d <- d[N > 0]
  return(d)
}

