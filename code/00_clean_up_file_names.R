library(here)
library(tidyverse)
library(fs)
library(glue)


# common function ---------------------------------------------------------

clean_file_names <- function(pattern) {
  og_file_paths <-
    list.files(here::here("raw_data"), pattern = pattern, full.name = TRUE)

  new_file_names <-
    tolower(basename(og_file_paths))

  new_file_names <-
    str_replace_all(new_file_names, " ", "_")

  return(new_file_names)
}


# See all old files in raw directory ------------------------------------------
og_file_paths <-
  list.files(here::here("raw_data"), full.name = TRUE)

flat_files <-
  og_file_paths[str_detect(og_file_paths, "Flat")]

label_files <-
  og_file_paths[str_detect(og_file_paths, "Label")]

spec_files <-
  og_file_paths[str_detect(og_file_paths, "Spec")]

crosswalk_files <-
  og_file_paths[str_detect(og_file_paths, "crosswalk")]


# make some folders to stick clean data into ------------------------------
patterns <-
  c("Flat", "Label", "Spec")

new_file_names <-
  map(patterns, ~ clean_file_names(.x))


walk(folders, ~ dir_create(path("raw_data", .x)))

# Tweak files --------------------------------------------------------
patterns <-
  c("Flat", "Label", "Spec")

new_file_names <-
  map(patterns, ~ clean_file_names(.x))

new_file_names <-
  map(new_file_names, ~ (tolower(basename(.x))))


# Write clean files -------------------------------------------------------

old_path <- dirname(all_files)

basenames <-
  map(
    list(flat_files, label_files, spec_files),
    basename
  )

walk2(
  basename(flat_files),
  new_file_names[[1]],
  ~ file_copy(
    path(here("raw_data", .x)),
    path(here("raw_data", "flat_files", .y))
  )
)

walk2(
  basename(label_files),
  new_file_names[[2]],
  ~ file_copy(
    path(here("raw_data", .x)),
    path(here("raw_data", "labels_specs", .y))
  )
)

walk2(
  basename(spec_files),
  new_file_names[[3]],
  ~ file_copy(
    path(here("raw_data", .x)),
    path(here("raw_data", "labels_specs", .y))
  )
)


# Remove old ugly files  --------------------------------------------------
walk(
  list(flat_files, label_files, spec_files),
  ~ file_delete(.x)
)
