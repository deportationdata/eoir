library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/utilities.R")

court_applications_tbl <-
  read_eoir_tsv("inputs_eoir/tbl_Court_Appln.csv") |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup table for validation
lkp_appln <- read_eoir_lookup("inputs_eoir/tblLookUp_Appln.csv")

# Validate before transforms
court_applications_tbl |>
  col_vals_not_null(IDNPROCEEDINGAPPLN) |>
  col_vals_not_null(IDNPROCEEDING) |>
  col_vals_not_null(IDNCASE) |>
  col_vals_regex(IDNPROCEEDINGAPPLN, "^\\d+$") |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$") |>
  col_vals_regex(IDNCASE, "^\\d+$") |>
  col_vals_not_null(APPL_CODE) |>
  col_vals_in_set(APPL_CODE, c(lkp_appln$strcode, NA)) |>
  col_vals_regex(APPL_RECD_DATE, "^\\d{4}-\\d{2}-\\d{2}", na_pass = TRUE)

court_applications_tbl <-
  court_applications_tbl |>
  janitor::clean_names() |>
  mutate(idncase = as.integer(idncase))

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
