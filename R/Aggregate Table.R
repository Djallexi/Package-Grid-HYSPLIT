aggregate_table <- function(dt, 
                                by_uid    = TRUE, 
                                by_month  = TRUE,
                                lat_col   = "lat",
                                lon_col   = "lon",
                                uid_col   = "uID",
                                month_col = "yearmonth",
                                value_col = "hyads") {
  
  # 1. Construction dynamique des colonnes de regroupement
  group_keys <- c(lat_col, lon_col)
  if (by_uid)   group_keys <- c(group_keys, uid_col)
  if (by_month) group_keys <- c(group_keys, month_col)
  
  # 2. Agrégation ultra-rapide avec .SD et .SDcols
  # Cela dit à data.table : "Applique la fonction sum sur la colonne 'value_col', 
  # en regroupant par 'group_keys'". 
  # L'avantage : GForce (l'optimiseur) est activé.
  res <- dt[, lapply(.SD, sum, na.rm = TRUE), by = group_keys, .SDcols = value_col]
  
  return(res)
}
