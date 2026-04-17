if (!requireNamespace("splitr", quietly = TRUE)) {
  pak::pak("rich-iannone/splitr")
} else {
  cat("splitr est déjà installé.\n")
}


# 1. Créer le répertoire lib
lib_dir <- path.expand("~/lib")
dir.create(lib_dir, recursive = TRUE, showWarnings = FALSE)

# 2. Télécharger et extraire libgfortran.so.3
url <- "http://archive.ubuntu.com/ubuntu/pool/universe/g/gcc-6/libgfortran3_6.4.0-17ubuntu1_amd64.deb"
deb_file <- file.path(lib_dir, "libgfortran3.deb")

cat("Téléchargement de libgfortran3...\n")
download.file(url, deb_file, mode = "wb")

# 3. Extraire le .deb
cat("Extraction...\n")
old_wd <- getwd()
setwd(lib_dir)

# Extraire avec ar et tar
system("ar x libgfortran3.deb")
system("tar xf data.tar.xz")

# 4. Copier la bibliothèque
file.copy(
  from = "usr/lib/x86_64-linux-gnu/libgfortran.so.3.0.0",
  to = "libgfortran.so.3.0.0",
  overwrite = TRUE
)

# Créer le lien symbolique
file.symlink("libgfortran.so.3.0.0", "libgfortran.so.3")

# 5. Nettoyer
unlink("usr", recursive = TRUE)
file.remove(c("control.tar.xz", "data.tar.xz", "debian-binary", "libgfortran3.deb"))

setwd(old_wd)

# 6. Vérifier
cat("\n✓ Installation terminée. Fichiers dans ~/lib:\n")
list.files(lib_dir, full.names = TRUE)

# 7. Vérifier les permissions
cat("\nPermissions:\n")
print(file.info(file.path(lib_dir, "libgfortran.so.3")))

# 8. Tester si HYSPLIT peut maintenant trouver la bibliothèque
cat("\nTest des dépendances HYSPLIT:\n")
abs_lib_dir <- normalizePath(lib_dir)
test_cmd <- paste0(
  "LD_LIBRARY_PATH=", abs_lib_dir, " ldd ",
  system.file("linux-amd64/hycs_std", package = "splitr")
)
system(test_cmd)

