## ---------------------------------------------------------------------------=
# Libs -----
## ---------------------------------------------------------------------------=
library(duckdb)
library(here)
library(tidyverse)
library(glue)
library(nanoparquet)


## ---------------------------------------------------------------------------=
# 1. Prep Imports -----
## ---------------------------------------------------------------------------=

flat_files <-
  list.files(
    here("raw_data", "flat_files"),
    pattern = "_puf_",
    full.names = TRUE
  )

## Get bones of dirname to spit out some files
dest_dir <-
  dirname(flat_files[[1]])

## Extract names for function / naming downstream parquet
tbl_names <-
  basename(flat_files)

## Shorten those names
tbl_names <- str_remove(tbl_names, "_flat_csv.zip")


## ---------------------------------------------------------------------------=
# 2. Unzip files --> write to parquet ----
## ---------------------------------------------------------------------------=

if (!dir.exists('raw_data/flat_files/parquet')) {
  fs::dir_create(fs::path('raw_data', 'flat_files', 'parquet'))
} else {
  print("Path already written")
}


tictoc::tic()
zip_to_parquet <-
  walk2(flat_files, tbl_names, function(table, tbl_name) {
    if (file.exists(glue('raw_data/flat_files/parquet/{tbl_name}.parquet'))) {
      cli::cli_alert_success('You have already done this')
    } else {
      cli::cli_inform(glue::glue('Reading {tbl_name}'))

      ## 2.1 Unzip file and read into memory :(
      raw_table <-
        unzip(table) %>%
        read_csv()

      cli::cli_inform(glue::glue('Writing {tbl_name} to parquet'))

      ## Write each file to parquet w/ nanoparquet dependency
      write_parquet(
        raw_table,
        glue('raw_data/flat_files/parquet/{tbl_name}.parquet')
      )

      cli::cli_inform(glue::glue('Deleting unzipped {tbl_name'))

      ## We'd want something here to delete unzipped files as we go....
    }
  })
tictoc::toc()


## ---------------------------------------------------------------------------=
# 3. Write new parquets to duckdb -----
## ---------------------------------------------------------------------------=

con <- dbConnect(duckdb(), 'duckdb/ahs.db')


## 3.1 Helper vectors for downstream pmap call ------
ahs_years <- seq(2007, 2023, by = 2)
metro_names <- paste("metro", ahs_years, sep = "_")
national_names <- paste("natl", ahs_years, sep = "_")

## If you want to see schemas....
## all_schemas <- DBI::dbGetQuery(con, "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA")

## 3.2 Get metro and national parquet patterns -----
metro_files <-
  list.files(
    here("raw_data", "flat_files", "parquet"),
    pattern = "metro",
    full.names = TRUE
  )

national_files <-
  list.files(
    here("raw_data", "flat_files", "parquet"),
    pattern = "national",
    full.names = TRUE
  )

## For eventual pmap / walk
metro_list <-
  list(metro_names, ahs_years, metro_files)


national_list <-
  list(national_names, ahs_years, national_files)


## ---------------------------------------------------------------------------=
# 3.3 duckd stuff ----
## ---------------------------------------------------------------------------=

## (Overkill, but seems like good for practicing with sql stuff)
dbExecute(con, "CREATE SCHEMA IF NOT EXISTS metro;")
dbExecute(con, "CREATE SCHEMA IF NOT EXISTS national;")

## Leaving this out for now... It says control has dupes, but it should be identifier
# ALTER TABLE metro.{tbl_name} ADD PRIMARY KEY (CONTROL);

metro_query <-
  "CREATE TABLE metro.{tbl_name} AS
SELECT *,
       {year} AS VINTAGE
FROM '{orig_path}';
        "

natl_query <-
  "CREATE TABLE national.{tbl_name} AS
SELECT *,
       {year} AS VINTAGE
FROM '{orig_path}';
        "
## 3.4 Call Queries w/ pmap (probs should be walk??) -------------
tictoc::tic()
pmap(metro_list, function(tbl_name, year, orig_path) {
  if (glue('{tbl_name}') %in% dbListTables(con)) {
    cli::cli_alert_success('Table Already Written')
  } else {
    cli::cli_inform(glue('Ingesting {tbl_name}'))

    dbExecute(con, sql(glue(metro_query)))
  }
})
tictoc::toc()


tictoc::tic()
pmap(national_list, function(tbl_name, year, orig_path) {
  if (glue('{tbl_name}') %in% dbListTables(con)) {
    cli::cli_alert_success('Table Already Written')
  } else {
    cli::cli_inform(glue('Ingesting {tbl_name}'))

    dbExecute(con, sql(glue(natl_query)))
  }
})

tictoc::toc()

## How you'd see columns within a schema that I made
# dbGetQuery(con, "SELECT column_name, data_type
#                  FROM information_schema.columns
#                  WHERE table_schema = 'metro' AND table_name = 'test'")
