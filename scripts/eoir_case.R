library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/utilities.R")

cases_tbl <- read_eoir_tsv("inputs_eoir/A_TblCase.csv")

# Fix mid-row tab shifts
# Two patterns: (1) shifts near ALIEN_CITY/STATE, (2) shifts elsewhere
case_generic_finder <- make_shift_finder(
  date_cols = c(
    "E_28_DATE",
    "LATEST_HEARING",
    "UP_BOND_DATE",
    "DATE_OF_ENTRY",
    "C_RELEASE_DATE",
    "ADDRESS_CHANGEDON",
    "DATE_DETAINED",
    "DATE_RELEASED",
    "DETENTION_DATE",
    "C_BIRTHDATE"
  ),
  non_date_cols = c(
    "Sex",
    "CUSTODY",
    "NAT",
    "LANG",
    "CASE_TYPE",
    "C_ASY_TYPE",
    "LPR",
    "CASEPRIORITY_CODE",
    "DETENTION_LOCATION",
    "DCO_LOCATION",
    "DETENTION_FACILITY_TYPE"
  )
)

case_shift_finder <- function(row_dt, n_extra) {
  # First try the ALIEN_CITY/STATE pattern (needs special CONCAT handling)
  alien_zip <- trimws(as.character(row_dt[["ALIEN_ZIPCODE"]]))
  alien_state <- trimws(as.character(row_dt[["ALIEN_STATE"]]))
  alien_city <- trimws(as.character(row_dt[["ALIEN_CITY"]]))
  if (
    !is.na(alien_zip) && nchar(alien_zip) > 0 && grepl("^[A-Z]{2}$", alien_zip)
  ) {
    if (is.na(alien_city) || nchar(alien_city) == 0) {
      return("ALIEN_CITY")
    }
    if (
      !is.na(alien_state) &&
        nchar(alien_state) > 0 &&
        !grepl("^[A-Z]{2}$", alien_state)
    ) {
      return("CONCAT_THEN_ALIEN_STATE")
    }
    return("ALIEN_STATE")
  }
  # Fall back to generic date/non-date mismatch detection
  case_generic_finder(row_dt, n_extra)
}

case_pre_fix <- function(dt, row_n, n_extra) {
  i <- dt[.(row_n), which = TRUE]
  city <- trimws(as.character(dt[i, ALIEN_CITY]))
  state <- trimws(as.character(dt[i, ALIEN_STATE]))
  set(dt, i, "ALIEN_CITY", paste(city, state))
}

cases_tbl <- auto_fix_tab_shifts(
  cases_tbl,
  case_shift_finder,
  pre_fix = case_pre_fix
)

cases_tbl <-
  cases_tbl |>
  as_tibble() |>
  clean_eoir_cols()

na_vals <- c("", "NA", "N/A", "NULL")

spec <- cols(
  IDNCASE = col_integer(),
  E_28_DATE = col_datetime(format = ""),
  LATEST_HEARING = col_datetime(format = ""),
  UP_BOND_DATE = col_datetime(format = ""),
  DATE_OF_ENTRY = col_datetime(format = ""),
  C_RELEASE_DATE = col_datetime(format = ""),
  ADDRESS_CHANGEDON = col_datetime(format = ""),
  DATE_DETAINED = col_datetime(format = ""),
  DATE_RELEASED = col_datetime(format = "")
)

cases_tbl <- type_convert(cases_tbl, col_types = spec, na = na_vals)

cases_tbl <-
  cases_tbl |>
  janitor::clean_names() |>
  select(
    -c(
      lpr,
      site_type,
      atty_nbr,
      update_site,
      latest_hearing,
      latest_time,
      latest_cal_type,
      up_bond_date,
      up_bond_rsn,
      correctional_fac,
      release_year,
      release_month,
      inmate_housing,
      updated_state,
      updated_city,
      updated_zipcode,
      alien_city,
      address_changedon,
      zbond_mrg_flag,
      dco_location,
      detention_facility_type
    )
  ) |>
  mutate(
    e_28_date = as.Date(e_28_date),
    birth_year = as.integer(str_extract(c_birthdate, "\\d{4}"))
  ) |>
  select(-c_birthdate) |>
  # Drop rows with remaining tab shifts that auto_fix missed
  # (detectable because sex gets a datetime string instead of F/M/N/U)
  filter(is.na(sex) | sex %in% c("F", "M", "N", "U"))

arrow::write_feather(
  cases_tbl,
  "tmp/cases_tmp.feather"
)
