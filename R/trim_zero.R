trim_zero <- function (Min) 
{
  M <- copy(Min)
  p_zero_df <- M[height == 0, ]
  particles <- unique(p_zero_df$particle_no)
  for (p in particles) {
    h_zero <- p_zero_df[particle_no == p, hour]
    M[particle_no == p & hour >= h_zero, ] <- NA
  }
  M <- na.omit(M)
  return(M)
}
