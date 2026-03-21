library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

cases_tbl <- read_eoir_tsv("inputs_eoir/A_TblCase.csv")

# Fix mid-row tab shifts
# Two patterns: (1) shifts near ALIEN_CITY/STATE, (2) shifts elsewhere
case_shift_finder <- local({
  date_cols <- c(
    "E_28_DATE",
    "LATEST_HEARING",
    "UP_BOND_DATE",
    "DATE_OF_ENTRY",
    "C_RELEASE_DATE",
    "ADDRESS_CHANGEDON",
    "DATE_DETAINED",
    "DATE_RELEASED",
    "DETENTION_DATE"
  )
  non_date_cols <- c(
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
  generic_finder <- make_shift_finder(date_cols, non_date_cols)

  function(row_dt, n_extra) {
    # First try the ALIEN_CITY/STATE pattern (needs special CONCAT handling)
    alien_zip <- trimws(as.character(row_dt[["ALIEN_ZIPCODE"]]))
    alien_state <- trimws(as.character(row_dt[["ALIEN_STATE"]]))
    alien_city <- trimws(as.character(row_dt[["ALIEN_CITY"]]))
    if (
      !is.na(alien_zip) &&
        nchar(alien_zip) > 0 &&
        grepl("^[A-Z]{2}$", alien_zip)
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
    generic_finder(row_dt, n_extra)
  }
})

case_pre_fix <- function(dt, row_n, n_extra) {
  i <- dt[.(row_n), which = TRUE]
  city <- trimws(as.character(dt[i, ALIEN_CITY]))
  state <- trimws(as.character(dt[i, ALIEN_STATE]))
  set(dt, i, "ALIEN_CITY", paste(city, state))
}

cases_tbl <-
  auto_fix_tab_shifts(
    cases_tbl,
    case_shift_finder,
    pre_fix = case_pre_fix
  )$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Pre-process ALIEN_ZIPCODE: strip ZIP+4 suffixes and pad leading zeros
cases_tbl <- cases_tbl |>
  mutate(
    ALIEN_ZIPCODE = str_extract(ALIEN_ZIPCODE, "^\\d{3,5}"),
    ALIEN_ZIPCODE = if_else(
      !is.na(ALIEN_ZIPCODE),
      str_pad(ALIEN_ZIPCODE, 5, pad = "0"),
      NA_character_
    )
  )

# Load lookup tables for validation
lkp_nat <- read_eoir_lookup("inputs_eoir/tblLookupAlienNat.csv")
lkp_lang <- read_eoir_lookup("inputs_eoir/tblLanguage.csv")
lkp_priority <- read_eoir_lookup("inputs_eoir/tblLookup_CasePriority.csv")
lkp_case_type <- read_eoir_lookup("inputs_eoir/tblLookupCaseType.csv")
lkp_sex <- read_eoir_lookup("inputs_eoir/tblLookupSex.csv")
lkp_custody <- read_eoir_lookup("inputs_eoir/tblLookupCustodyStatus.csv")

# Validate that shift-fixing didn't corrupt key columns
cases_tbl |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    Sex,
    c(lkp_sex$strcode, "N", "U", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    C_ASY_TYPE,
    c("E", "I", "J", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    CASE_TYPE,
    c(lkp_case_type$str_code, "BND", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    CUSTODY,
    c(lkp_custody$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    LPR,
    c("0", "1", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    NAT,
    c(lkp_nat$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    LANG,
    c(lkp_lang$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    CASEPRIORITY_CODE,
    c(lkp_priority$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    ALIEN_STATE,
    "^[A-Z]{2}$",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_regex(
    ALIEN_ZIPCODE,
    "^\\d{5}$",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_regex(
    DETENTION_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

cases_tbl <- fast_convert(
  cases_tbl,
  list(
    IDNCASE = "integer",
    E_28_DATE = "datetime",
    LATEST_HEARING = "datetime",
    UP_BOND_DATE = "datetime",
    DATE_OF_ENTRY = "datetime",
    C_RELEASE_DATE = "datetime",
    ADDRESS_CHANGEDON = "datetime",
    DATE_DETAINED = "datetime",
    DATE_RELEASED = "datetime"
  )
)

cases_tbl <-
  cases_tbl |>
  janitor::clean_names() |>
  rename(
    asylum_claim_type = c_asy_type,
    sex_code = sex,
    case_priority_code = casepriority_code,
    e28_date = e_28_date
  ) |>
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
      alien_state,
      address_changedon,
      zbond_mrg_flag,
      dco_location,
      detention_facility_type,
      case_type,
      c_release_date,
      date_detained,
      date_released,
      detention_date,
      detention_location
    )
  ) |>
  mutate(
    birth_year = as.integer(str_extract(c_birthdate, "\\d{4}"))
  ) |>
  select(-c_birthdate)

# Post-transform validation
cases_tbl |>
  col_vals_between(
    birth_year,
    1900L,
    as.integer(format(Sys.Date(), "%Y")),
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

arrow::write_parquet(
  cases_tbl,
  "tmp/cases_tmp.parquet"
)
