library(tidyverse)
library(tidylog)
library(data.table)

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

cases_tbl <-
  as_tibble(cases_tbl) |>
  # remove extra columns created because of malformed rows
  select(-matches("^V\\d+$"))

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
