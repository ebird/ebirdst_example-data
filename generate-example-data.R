library(terra)
library(fs)
library(sf)
library(rnaturalearth)
library(tidyverse)
library(glue)
library(jsonlite)
library(DBI)
library(RSQLite)
library(ebirdst)

year <- "2021"
species <- "yebsap"
ex_species <- paste0(species, "-example")
# source data
sp_path <- "/ocean/projects/deb200005p/sligocki/stem_hwf/ERD2021/land_migrant/yebsap/results/ebirdst_package"
set.seed(1)

# destination
ex_dir <- path("/ocean/projects/deb200005p/mstrimas/ebirdst/2021/yepsap-example")
ex_weekly_dir <- path(ex_dir, "weekly")
ex_seasonal_dir <- path(ex_dir, "seasonal")
ex_ranges_dir <- path(ex_dir, "ranges")
dir_create(ex_dir)
dir_create(ex_weekly_dir)
dir_create(ex_seasonal_dir)
dir_create(ex_ranges_dir)

# copy config file and databases
dir_ls(sp_path, regexp = "(json|db)$", recurse = FALSE, type = "file") %>%
  file_copy(ex_dir)

# change species code
p <- read_json(path(ex_dir, "config.json"), simplifyVector = TRUE)
p[["SPECIES_CODE"]] <- ex_species
write_json(p, path(ex_dir, "config.json"),
           pretty = TRUE, digits = NA)

# region for subsetting from rnaturalearth
state_mi_ll <- ne_states(iso_a2 = "US", returnclass = "sf") %>%
  filter(name == "Michigan") %>%
  st_geometry()
bb <- st_bbox(state_mi_ll)

# crop and mask rasters
tifs <- dir_ls(sp_path, regexp = "_lr_.*tif$", recurse = TRUE, type = "file")
sinu_crs <- st_crs(rast(tifs[1]))
state_mi <- state_mi_ll %>%
  st_transform(crs = sinu_crs) %>%
  vect()
for (f in tifs) {
  f_out <- str_replace_all(f, sp_path, ex_dir) %>%
    str_replace_all(paste0(species, "_"), paste0(ex_species, "_"))
  r_ex <- f %>%
    rast() %>%
    mask(state_mi) %>%
    writeRaster(filename = f_out, overwrite = TRUE,
                datatype = "FLT4S",
                gdal = c("COMPRESS=DEFLATE",
                         "TILED=YES",
                         "COPY_SRC_OVERVIEWS=YES"))
}

# copy csv files and geopackages
dir_ls(path(sp_path, "weekly"), regexp = "csv$", recurse = FALSE, type = "file") %>%
  file_copy(ex_weekly_dir)
dir_ls(path(sp_path, "seasonal"), regexp = "csv$", recurse = FALSE, type = "file") %>%
  file_copy(ex_seasonal_dir)
dir_ls(path(sp_path, "ranges"), regexp = "_lr_.*gpkg$", recurse = FALSE, type = "file") %>%
  file_copy(ex_ranges_dir)
# fix species code
to_copy <- dir_ls(ex_dir, recurse = TRUE, regexp = paste0("/", species, "_"))
to_copy %>%
  str_replace_all(paste0("/", species, "_"), paste0("/", ex_species, "_")) %>%
  file_move(to_copy, .)

# database connections
pipd_db <- path(ex_dir, "stixel_summary.db")
pred_db <- path(ex_dir, "predictions.db")
pipd_con <- dbConnect(SQLite(), pipd_db)
pred_con <- dbConnect(SQLite(), pred_db)

# subset sqlite dbs to focal area
# predictions
sql <- str_glue("DELETE FROM predictions ",
                "WHERE longitude < {bb['xmin']} OR longitude > {bb['xmax']} ",
                "OR latitude < {bb['ymin']} OR latitude > {bb['ymax']};")
dbSendStatement(pred_con, sql)
# get sampling event ids that fall in polygon
sid <- tbl(pred_con, "predictions") %>%
  select(checklist_id, obs, latitude, longitude, day_of_year, year) %>%
  collect() %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326, remove = FALSE) %>%
  st_intersection(state_mi_ll) %>%
  st_drop_geometry()
# grid sample by year and case to reduce sample size
n_target <- 10000
prop_target <- min(n_target, nrow(sid)) / nrow(sid)
sid_sampled <- sid %>%
  filter(obs == 0) %>%
  split(.$year) %>%
  map_dfr(slice_sample, prop = prop_target) %>%
  bind_rows(filter(sid, obs > 0))
copy_to(pred_con, select(sid_sampled, checklist_id), name = "sid")
# drop rows from other tables out of bounds of
sql <- str_glue("DELETE FROM predictions WHERE checklist_id NOT IN ",
                "(SELECT checklist_id FROM sid);")
dbSendStatement(pred_con, sql)
dbSendStatement(pred_con, "DROP TABLE sid;")
dbSendStatement(pred_con, "vacuum;")
dbDisconnect(pred_con)

# pipd
sql <- str_glue("DELETE FROM stixel_summary ",
                "WHERE longitude < {bb['xmin']} OR longitude > {bb['xmax']} ",
                "OR latitude < {bb['ymin']} OR latitude > {bb['ymax']};")
dbSendStatement(pipd_con, sql)
# get stixels centroids that fall in polygon
# only retain 500 stixels
stixels <- tbl(pipd_con, "stixel_summary") %>%
  select(stixel_id, longitude, latitude) %>%
  collect() %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  st_intersection(state_mi_ll) %>%
  st_drop_geometry() %>%
  sample_n(size = min(500, nrow(.)))
copy_to(pipd_con, stixels)
# drop rows from other tables without out of bounds stixel centroids
for (t in c("occurrence_pds", "occurrence_pis",
            "count_pds", "count_pis",
            "stixel_summary")) {
  message("Processing ", t)
  sql <- str_glue("DELETE FROM {t} WHERE stixel_id NOT IN ",
                  "(SELECT stixel_id FROM stixels);")
  dbSendStatement(pipd_con, sql)
}
dbSendStatement(pipd_con, "DROP TABLE stixels;")
dbSendStatement(pipd_con, "vacuum;")
dbDisconnect(pipd_con)


# copy to repo dir ---
ebirdst_dir <- path(ebirdst_data_dir(), year, ex_species)
dir_create(ebirdst_dir)
repo_dir <- path("example-data", year)
dir_create(repo_dir)
glue("rsync -av bridges_data:{ex_dir}/ {ebirdst_dir}/") %>%
  system()
dir_copy(ebirdst_dir, repo_dir)

# file sizes must be below 50 mb
sizes <- dir_ls(repo_dir, type = "file", recurse = TRUE) %>%
  file_size() %>%
  as.numeric() %>%
  units::set_units("bytes")
stopifnot(sizes < units::set_units(50, "megabytes"))

# file list
file_list <- dir_ls(repo_dir, type = "file", recurse = TRUE) %>%
  str_remove("example-data/")
write_lines(file_list, path(repo_dir,"file-list.txt"))
