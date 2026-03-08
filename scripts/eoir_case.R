library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/eoir_utils.R")

cases_tbl <-
  data.table::fread(
    "inputs_eoir/A_TblCase.csv",
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = c("", "NA", "N/A", "NULL"),
    colClasses = "character",
    fill = 47,
    showProgress = FALSE
  )

case_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^A_TblCase\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(nrow(cases_tbl) == case_count)

# Fix mid-row tab shifts (~97 rows with extra tabs near ALIEN_CITY/STATE)
case_shift_finder <- function(row_dt, n_extra) {
  alien_zip <- trimws(as.character(row_dt[["ALIEN_ZIPCODE"]]))
  alien_state <- trimws(as.character(row_dt[["ALIEN_STATE"]]))
  alien_city <- trimws(as.character(row_dt[["ALIEN_CITY"]]))
  if (!is.na(alien_zip) && nchar(alien_zip) > 0 && grepl("^[A-Z]{2}$", alien_zip)) {
    if (is.na(alien_city) || nchar(alien_city) == 0) return("ALIEN_CITY")
    if (!is.na(alien_state) && nchar(alien_state) > 0 && !grepl("^[A-Z]{2}$", alien_state))
      return("CONCAT_THEN_ALIEN_STATE")
    return("ALIEN_STATE")
  }
  NA_character_
}

case_pre_fix <- function(dt, row_n, n_extra) {
  i <- dt[.(row_n), which = TRUE]
  city <- trimws(as.character(dt[i, ALIEN_CITY]))
  state <- trimws(as.character(dt[i, ALIEN_STATE]))
  set(dt, i, "ALIEN_CITY", paste(city, state))
}

cases_tbl <- auto_fix_tab_shifts(cases_tbl, case_shift_finder, pre_fix = case_pre_fix)

cases_tbl <-
  cases_tbl |>
  as_tibble() |>
  clean_string_cols() |>
  drop_overflow_cols()

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
  select(-c_birthdate)

arrow::write_feather(
  cases_tbl,
  "tmp/cases_tmp.feather"
)
