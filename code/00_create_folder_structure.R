folder_names <-
  c("raw_data", "code", "duckdb", "reports", "results", "talks")


purrr::walk(folder_names, ~ fs::dir_create(.x))
