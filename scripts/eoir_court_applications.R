library(tidyverse)
library(tidylog)

source("scripts/eoir_utils.R")

court_applications_tbl <- data.table::fread(
  "inputs_eoir/tbl_Court_Appln.csv",
  fill = 7
)

court_applications_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^tbl_Court_Appln\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(nrow(court_applications_tbl) == court_applications_count)

court_applications_tbl <-
  court_applications_tbl |>
  as_tibble() |>
  clean_string_cols() |>
  drop_overflow_cols() |>
  janitor::clean_names()

library(data.table)
setDT(court_applications_tbl)
court_applications_by_case <-
  court_applications_tbl[,
    .(
      asylumapp = any(appl_code == "ASYL"),
      withholdapp = any(appl_code == "ASYW"),
      catapp = any(appl_code == "WCAT"),
      adjustapp = any(appl_code == "245"),
      nonlprcancelapp = any(appl_code == "42B"),
      lprcancelapp = any(appl_code == "42A"),
      anyreliefapp = any(!is.na(appl_code) & appl_code != "VD")
    ),
    by = idncase
  ] |>
  as_tibble() |>
  filter(!is.na(idncase))

arrow::write_feather(
  court_applications_by_case,
  "tmp/court_applications_cases.feather"
)
