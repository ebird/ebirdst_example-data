library(arrow)
library(ebirdst)
library(fs)
library(glue)
library(jsonlite)
library(rnaturalearth)
library(sf)
library(terra)
library(tidyverse)

year <- "2022"
species <- "yebsap"
ex_species <- paste0(species, "-example")
# full species package directory
temp_dir <- path(tempdir(), ex_species)
dir_create(temp_dir)
# example data directory
dest_dir <- path("example-data", year)
dir_create(dest_dir)

# download full ebirdst package
s3_bucket <- Sys.getenv("EBIRDST_S3_BUCKET")
glue("aws s3 sync {s3_bucket}/{year}/{species}/ {temp_dir}/") %>%
  system()

# create directory structure
dirs <- c("weekly", "seasonal", "trends", "ranges", "pis", "ppms")
dirs <- path(dest_dir, ex_species, dirs) %>%
  as.character() %>%
  setNames(dirs)
walk(dirs, dir_create)

# all files to copy
files <- dir_ls(temp_dir, recurse = TRUE, type = "file") %>%
  # only retain lowest resolution
  discard(str_detect, pattern = "_(3|9)km_") %>%
  # remove web package
  discard(str_detect, pattern = "/web_download/") %>%
  # address pis separately
  discard(str_detect, pattern = "/pis/")
# only retain the top pis
top_preds <- c("elevation_250m_median",
               "ntl_mean",
               "gsw_c2_pland",
               "mcd12q1_lccs1_c21_pland",
               "mcd12q1_lccs1_c22_pland")
top_pred_pattern <- top_preds %>%
  str_replace_all("_", "-") %>%
  paste(collapse = "|")
files <- dir_ls(path(temp_dir, "pis"), recurse = TRUE, type = "file") %>%
  keep(str_detect, glue("({top_pred_pattern})")) %>%
  c(files, .)
# define source and destination
files <- tibble(source = files,
                destination = files %>%
                  str_replace(temp_dir, path(dest_dir, ex_species)) %>%
                  str_replace_all(paste0(species, "_"),
                                  paste0(ex_species, "_")))

# region for subsetting from rnaturalearth
boundary_ll <- ne_states(iso_a2 = "US", returnclass = "sf") %>%
  filter(name == "Michigan") %>%
  st_geometry()

# loop over files, copying and modifying as needed
for (i in seq_len(nrow(files))) {
  s <- files$source[i]
  d <- files$destination[i]
  if (str_ends(s, "config.json")) {
    p <- read_json(s, simplifyVector = TRUE)
    p[["SPECIES_CODE"]] <- ex_species
    write_json(p, path = d, pretty = TRUE, digits = NA, na = "null")
  } else if (str_ends(s, "tif")) {
    r <- rast(s)
    boundary <- vect(boundary_ll) %>%
      project(crs(r))
    r %>%
      crop(boundary) %>%
      mask(boundary) %>%
      writeRaster(filename = d, overwrite = TRUE,
                  datatype = "FLT4S",
                  gdal = c("COMPRESS=DEFLATE",
                           "TILED=YES",
                           "COPY_SRC_OVERVIEWS=YES"))
  } else if (str_ends(s, "gpkg")) {
    for (l in st_layers(s)$name) {
      read_sf(s, layer = l) %>%
        st_intersection(boundary_ll) %>%
        write_sf(d, layer = l, delete_layer = TRUE)
    }
  } else if (str_ends(s, "parquet")) {
    data <- read_parquet(s) %>%
      mutate(species_code = ex_species)
    data_sf <- st_as_sf(data, coords = c("longitude", "latitude"), crs = 4326)
    within <- st_intersects(data_sf, boundary_ll, sparse = FALSE)[, 1, drop = TRUE]
    write_parquet(data[within, ], d)
  } else if (str_ends(s, "csv")) {
    data <- read_csv(s, show_col_types = FALSE, na = "")
    if ("species_code" %in% names(data)) {
      data$species_code <- ex_species
    }
    if ("region_code" %in% names(data)) {
      data <- filter(data, region_code %in% c("USA", "USA-MI"))
    }
    write_csv(data, file = d, na = "")
  } else {
    file_copy(s, d, overwrite = TRUE)
  }
}

# file sizes must be below 50 mb
sizes <- path(dest_dir, ex_species) %>%
  dir_ls(type = "file", recurse = TRUE) %>%
  file_size() %>%
  as.numeric() %>%
  units::set_units("bytes")
stopifnot(sizes < units::set_units(50, "megabytes"))

# file list
file_list <- path(dest_dir, ex_species) %>%
  dir_ls(type = "file", recurse = TRUE) %>%
  str_remove("example-data/")
write_lines(file_list, path(dest_dir, "file-list.txt"))
