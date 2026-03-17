library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

# Fix mid-row tab shifts
charges_shift_finder <- make_shift_finder(
  date_cols = character(0),
  non_date_cols = c("CHARGE", "CHG_STATUS")
)

charges_raw <- read_eoir_tsv("inputs_eoir/B_TblProceedCharges.csv")

charges_fix_result <- auto_fix_tab_shifts(charges_raw, charges_shift_finder)

charges_tbl <-
  charges_fix_result$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Validate before transforms
charges_tbl |>
  col_vals_not_null(
    IDNPRCDCHG,
    actions = action_levels(warn_at = 0.005, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.001)
  ) |>
  col_vals_not_null(
    IDNPROCEEDING,
    actions = action_levels(warn_at = 0.005, stop_at = 0.001)
  ) |>
  col_vals_regex(IDNPRCDCHG, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_not_null(
    CHARGE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.0001)
  ) |>
  col_vals_regex(
    CHARGE,
    "^(212|237|241|242|246|215)[a-zA-Z]\\d",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    CHG_STATUS,
    c("N", "O", "S", "W", "s", "w", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

charges_tbl <- type_convert(
  charges_tbl,
  col_types = cols(
    IDNPRCDCHG = col_integer(),
    IDNCASE = col_integer(),
    IDNPROCEEDING = col_integer()
  ),
  na = na_vals
)

check_parse(charges_tbl)

charges_tbl <-
  charges_tbl |>
  janitor::clean_names()

charges_dt <- as.data.table(charges_tbl)

setorder(charges_dt, idncase, idnproceeding, idnprcdchg)

charges_by_case <- charges_dt[,
  .(
    charge_code_1 = charge[1L],
    charge_code_2 = charge[2L],
    charge_code_3 = charge[3L],
    charge_code_4 = charge[4L],
    charges_all = paste(charge, collapse = "; ")
  ),
  by = idncase
]

arrow::write_parquet(
  charges_by_case,
  "tmp/charges_cases.parquet"
)
