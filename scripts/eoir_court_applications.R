library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

court_appln_shift_finder <- make_shift_finder(
  date_cols = c("APPL_RECD_DATE"),
  non_date_cols = c("APPL_CODE", "APPL_DEC")
)

court_applications_raw <- read_eoir_tsv("inputs_eoir/tbl_Court_Appln.csv")

appln_fix_result <- auto_fix_tab_shifts(
  court_applications_raw,
  court_appln_shift_finder
)

court_applications_tbl <-
  appln_fix_result$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup tables for validation
lkp_appln <- read_eoir_lookup("inputs_eoir/tblLookUp_Appln.csv")
lkp_appl_dec <- read_eoir_lookup("inputs_eoir/tblLookupCourtAppDecisions.csv")

# Validate before transforms
court_applications_tbl |>
  col_vals_not_null(
    IDNPROCEEDINGAPPLN,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNPROCEEDING,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNPROCEEDINGAPPLN, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    APPL_CODE,
    c(lkp_appln$strcode, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_in_set(
    APPL_DEC,
    c(lkp_appl_dec$str_court_appln_dec_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_regex(
    APPL_RECD_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

court_applications_tbl <- type_convert(
  court_applications_tbl,
  col_types = cols(
    IDNPROCEEDINGAPPLN = col_integer(),
    IDNPROCEEDING = col_integer(),
    IDNCASE = col_integer(),
    APPL_RECD_DATE = col_date()
  ),
  na = na_vals
)

check_parse(court_applications_tbl)

court_applications_tbl <-
  court_applications_tbl |>
  janitor::clean_names()

setDT(court_applications_tbl)

court_applications_by_case <-
  court_applications_tbl[,
    .(
      asylum_application = any(appl_code == "ASYL"),
      withholding_application = any(appl_code == "ASYW"),
      cat_application = any(appl_code == "WCAT"),
      adjustment_application = any(appl_code == "245"),
      non_lpr_cancellation_application = any(appl_code == "42B"),
      lpr_cancellation_application = any(appl_code == "42A"),
      any_relief_application = any(!is.na(appl_code) & appl_code != "VD")
    ),
    by = idncase
  ] |>
  filter(!is.na(idncase))

arrow::write_parquet(
  court_applications_by_case,
  "tmp/court_applications_cases.parquet"
)
