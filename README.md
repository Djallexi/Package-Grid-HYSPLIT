# UpdatedDisperseR

R package for running HYSPLIT dispersion simulations at scale and computing exposure grids from emission sources (coal plants, industrial units, etc.).

**Pipeline:** Emission sources → HYSPLIT simulations → Linked grids → Exposure data frame

---

## Requirements

- R ≥ 4.1
- Linux (Ubuntu/Debian recommended) or macOS
- ~5 GB free disk space for meteorological data

---

## Installation

### Step 1 — Install the package

```r
# Installer pak (une seule fois)
install.packages("pak")

# Installer ton package depuis GitHub avec pak
pak::pak("Djallexi/Package-Grid-HYSPLIT")

# Charger le package
library(UpdatedDisperseR)
```

### (Optionnal) Step 1 bis — Set your GitHub token (if needed)

If you clusterise multiple UpdatedDisperseR R session and want to launch simulations during a short time periode. A GitHub PAT can be required.

**Create a token:** https://github.com/settings/tokens → Generate new token (classic) → check `repo` scope

**Save it permanently** (never write it in your scripts):

```r
# Opens ~/.Renviron in your editor
usethis::edit_r_environ()
```

Add this line to the file:
```
GITHUB_PAT=ghp_your_token_here
```

Save, then **restart R**.

### Step 3 — Install system dependency (Linux only) and Splitrs

HYSPLIT requires `libgfortran3`, which is not available on recent Ubuntu/Debian by default:

```r
library(UpdatedDisperseR)

#Install system dependency
install_system_deps()

#Install SplitR
pak::pak('rich-iannone/splitr')
```

> **macOS:** this step installs `gcc` via Homebrew automatically.  
> **Windows:** not supported — use WSL2 (Ubuntu) instead.

### Step 4 — Open vignettes

vignette("get-started", package = "UpdatedDisperseR")

### Step 5 — Create the working directory structure

```r
UpdatedDisperseR::create_dirs("~/work")
```

This creates:

```
~/work/
  main/
    input/
      hpbl/            ← put hpbl.mon.mean.nc here
      Plant Units/     ← put your units CSV here
    output/
      linked_grids/
      exposure/
      logs/
```

### Step 6 — Add your input files

| File | Where to put it | Description |
|---|---|---|
| `hpbl.mon.mean.nc` | `~/work/main/input/hpbl/` | Monthly PBL height (NCEP/NCAR). Download from https://psl.noaa.gov/data/gridded/data.ncep.reanalysis.derived.html |
| `your_units.csv` | `~/work/main/input/Plant Units/` | Your emission sources — must include columns: `ID`, `year`, `Latitude`, `Longitude`, `totcap`, `SO2_total_kt`, `Type.of.Coal` |

---

## Step 7 - Running a simulation

Open `inst/scripts/Disperser Year Job.R` and edit the `CONFIG` block at the top:

```r
TARGET_YEAR <- 2022    # ← set your year

CONFIG <- list(
  units_file     = "~/work/main/input/Plant Units/your_file.csv",
  pblheight_file = "~/work/main/input/hpbl/hpbl.mon.mean.nc",

  startday       = paste0(TARGET_YEAR, "-01-01"),  # simulation start
  endday         = paste0(TARGET_YEAR, "-12-31"),  # simulation end
  duration       = 120,    # hours per trajectory
  npart          = 100,    # particles per simulation
  numberofplants = 50,     # top N emitters to simulate

  cores          = parallel::detectCores() - 1
)
```

Then run the script. It will automatically:
1. Download missing meteorological files
2. Run HYSPLIT simulations in parallel
3. Link trajectories to a 0.1° grid
4. Calculate monthly exposure per unit

**Outputs** (in `~/work/main/output/`):

```
linked_grids/
  linked_grids_2022_native_withID.csv.gz   ← particle counts per grid cell
exposure/
  exposure_monthly_2022_unit.csv.gz        ← weighted exposure per grid cell
```

---

Now you have your csv results, there is additionnal function you could use

## Bonus Step - Visualisation

```r
library(UpdatedDisperseR)
library(data.table)

# Load results
exp <- fread("~/work/main/output/exposure/exposure_monthly_2022_unit.csv.gz")

# Plot April 2022
plot_impact_sf_academic(
  df               = exp,
  year             = 2022,
  month            = 4,
  metric_col       = "hyads",
  uid_col          = "uID",
  uid_filter       = NULL,       # NULL = all units
  color_palette    = "sentinel",
  zoom             = TRUE,
  xlim_manual      = c(20, 140),
  ylim_manual      = c(-10, 60),
  graph.dir        = "~/work/main/output/graph"
)
```

## Bonus Step - Aggregation

```r
# Aggregate by month (sum across all units)
df_month <- aggregate_hyads_dt(exp,
  by_uid    = FALSE,
  by_month  = TRUE,
  lat_col   = "lat", lon_col = "lon",
  uid_col   = "uID", month_col = "yearmonth",
  value_col = "hyads"
)

# Aggregate by unit (sum across all months)
df_uid <- aggregate_hyads_dt(exp,
  by_uid    = TRUE,
  by_month  = FALSE,
  lat_col   = "lat", lon_col = "lon",
  uid_col   = "uID", month_col = "yearmonth",
  value_col = "hyads"
)
```

---

## Troubleshooting

**`could not find function "create_dirs"`**  
→ You forgot `library(UpdatedDisperseR)` before calling the function.

**`libgfortran.so.3: cannot open shared object file`**  
→ Run `UpdatedDisperseR::install_system_deps()` then restart R.

**`GITHUB_PAT non défini`**  
→ Follow Step 2 above. The token must be in `~/.Renviron`, not in the script.

**Met files not downloading**  
→ Check your internet connection and that `GITHUB_PAT` is set. Files are downloaded to `/var/tmp/meteo_data/` by default (configurable in `CONFIG$meteo_dir`).

---

## Citation

If you use this package in your research, please cite:  
> [your citation here]
