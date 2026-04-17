# R/zzz.R
.onAttach <- function(libname, pkgname) {
  # Vérifier libgfortran3
  lib_dir <- path.expand("~/lib")
  so_paths <- c(
    file.path(lib_dir, "libgfortran.so.3"),
    "/usr/lib/x86_64-linux-gnu/libgfortran.so.3",
    "/usr/lib/libgfortran.so.3"
  )

  if (!any(file.exists(so_paths))) {
    packageStartupMessage(
      "⚠️  UpdatedDisperseR : libgfortran3 introuvable.\n",
      "   Lancez : UpdatedDisperseR::install_system_deps()\n",
      "   avant d'utiliser run_disperser_parallel()."
    )
  }

  # Vérifier splitr
  if (!requireNamespace("splitr", quietly = TRUE)) {
    packageStartupMessage(
      "⚠️  UpdatedDisperseR : splitr non installé.\n",
      "   Lancez : pak::pak('rich-iannone/splitr')"
    )
  }
}
