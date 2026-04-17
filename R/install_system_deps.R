# R/install_system_deps.R — à mettre dans le package
install_system_deps <- function(lib_dir = path.expand("~/lib"), force = FALSE) {

  os <- Sys.info()["sysname"]

  # 1. Vérifier si déjà installé
  already_installed <- function() {
    paths <- c(
      file.path(lib_dir, "libgfortran.so.3"),
      "/usr/lib/x86_64-linux-gnu/libgfortran.so.3",
      "/usr/lib/libgfortran.so.3"
    )
    any(file.exists(paths))
  }

  if (!force && already_installed()) {
    message("✓ libgfortran3 déjà disponible — rien à faire.")
    return(invisible(TRUE))
  }

  if (os == "Windows") {
    stop(
      "Windows détecté. HYSPLIT sur Windows n'a pas besoin de libgfortran3.\n",
      "Téléchargez HYSPLIT directement : https://www.ready.noaa.gov/HYSPLIT.php"
    )
  }

  if (os == "Darwin") {  # macOS
    message("macOS détecté. Installation via Homebrew...")
    if (system("which brew", ignore.stdout = TRUE) != 0)
      stop("Homebrew non trouvé. Installez-le depuis https://brew.sh")
    system("brew install gcc")
    # Créer lien symbolique vers la lib gcc de brew
    gfortran_path <- system("brew --prefix gcc", intern = TRUE)
    lib_path <- Sys.glob(file.path(gfortran_path, "lib/gcc/*/libgfortran.3.dylib"))[1]
    if (!is.na(lib_path)) {
      dir.create(lib_dir, recursive = TRUE, showWarnings = FALSE)
      file.symlink(lib_path, file.path(lib_dir, "libgfortran.so.3"))
    }
    return(invisible(TRUE))
  }

  # Linux
  arch <- system("uname -m", intern = TRUE)

  # 1. Essayer apt d'abord (propre, gère les dépendances)
  if (system("which apt-get", ignore.stdout = TRUE) == 0) {
    message("Tentative via apt-get...")
    ret <- system("apt-get install -y libgfortran3 2>/dev/null || apt-get install -y libgfortran5 2>/dev/null")
    if (ret == 0) {
      message("✓ Installé via apt-get")
      return(invisible(TRUE))
    }
    message("apt-get échoué (pas de sudo ?), fallback manuel...")
  }

  # 2. Fallback : téléchargement manuel
  arch_deb <- switch(arch,
                     "x86_64"  = "amd64",
                     "aarch64" = "arm64",
                     stop("Architecture non supportée : ", arch)
  )

  url <- paste0(
    "https://archive.ubuntu.com/ubuntu/pool/universe/g/gcc-6/",
    "libgfortran3_6.4.0-17ubuntu1_", arch_deb, ".deb"
  )

  dir.create(lib_dir, recursive = TRUE, showWarnings = FALSE)
  deb_file <- file.path(lib_dir, "libgfortran3.deb")

  message("Téléchargement libgfortran3 (", arch_deb, ")...")
  tryCatch(
    download.file(url, deb_file, mode = "wb", quiet = FALSE),
    error = function(e) stop("Échec du téléchargement : ", e$message)
  )

  old_wd <- getwd()
  on.exit(setwd(old_wd))  # <-- important : toujours restaurer le wd
  setwd(lib_dir)

  # Vérifier que ar et tar sont disponibles
  for (cmd in c("ar", "tar")) {
    if (system(paste("which", cmd), ignore.stdout = TRUE) != 0)
      stop(cmd, " introuvable. Installez binutils.")
  }

  system("ar x libgfortran3.deb")

  # data.tar peut être .xz ou .zst selon la version
  data_tar <- if (file.exists("data.tar.xz")) "data.tar.xz" else "data.tar.zst"
  system(paste("tar xf", data_tar))

  so_glob <- Sys.glob(paste0("usr/lib/*/libgfortran.so.3.0.0"))
  if (length(so_glob) == 0) stop("libgfortran.so.3.0.0 introuvable dans le .deb")

  file.copy(so_glob[1], "libgfortran.so.3.0.0", overwrite = TRUE)
  file.symlink("libgfortran.so.3.0.0", "libgfortran.so.3")

  # Nettoyage
  unlink("usr", recursive = TRUE)
  file.remove(c("control.tar.xz", data_tar, "debian-binary", "libgfortran3.deb",
                "_gpgbuilder")[file.exists(c("control.tar.xz", data_tar,
                                             "debian-binary", "libgfortran3.deb", "_gpgbuilder"))])

  message("✓ libgfortran3 installé dans : ", lib_dir)
  invisible(TRUE)
}
