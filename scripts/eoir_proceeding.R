library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/utilities.R")

proceeding_tbl <- read_eoir_tsv("inputs_eoir/B_TblProceeding.csv")

# Fix mid-row tab shifts (~71 rows with extra tabs before COMP_DATE/APPEAL area)
proc_shift_finder <- make_shift_finder(
  date_cols = c(
    "OSC_DATE",
    "INPUT_DATE",
    "TRANS_IN_DATE",
    "HEARING_DATE",
    "COMP_DATE",
    "VENUE_CHG_GRANTED",
    "DATE_APPEAL_DUE_STATUS",
    "DATE_DETAINED",
    "DATE_RELEASED"
  ),
  non_date_cols = c(
    "CUSTODY",
    "CASE_TYPE",
    "NAT",
    "LANG",
    "ABSENTIA",
    "TRANSFER_STATUS"
  )
)

proceeding_tbl <- auto_fix_tab_shifts(proceeding_tbl, proc_shift_finder)

na_vals <- c("", "NA", "N/A", "NULL")

proceeding_tbl <-
  proceeding_tbl |>
  as_tibble() |>
  clean_eoir_cols() |>
  type_convert(
    col_types = cols(
      IDNPROCEEDING = col_integer(),
      IDNCASE = col_integer(),
      OSC_DATE = col_datetime(format = ""),
      INPUT_DATE = col_datetime(format = ""),
      TRANS_IN_DATE = col_datetime(format = ""),
      HEARING_DATE = col_datetime(format = ""),
      COMP_DATE = col_datetime(format = ""),
      VENUE_CHG_GRANTED = col_datetime(format = ""),
      DATE_APPEAL_DUE_STATUS = col_datetime(format = ""),
      AGGRAVATE_FELON = col_logical(),
      DATE_DETAINED = col_datetime(format = ""),
      DATE_RELEASED = col_datetime(format = "")
    ),
    na = na_vals
  )

cases_from_proceedings <-
  proceeding_tbl |>
  janitor::clean_names() |>
  mutate(
    osc_date = as.Date(osc_date),
    comp_date = as.Date(comp_date)
  ) |>
  # clean up absentia column which has erroneous values due to csv errors
  # assumes missing values, date errors, and "X", "DEP", and "5" values are not absentia
  mutate(
    absentia = case_when(absentia == "Y" ~ TRUE, TRUE ~ FALSE)
  ) |>
  # drop rows with missing IDNCASE (creating a case-level dataset)
  # -2 rows
  filter(!is.na(idncase)) |>
  # drop cases with inconsistent case types
  # -314 rows
  filter(n_distinct(case_type) == 1, .by = "idncase") |>
  arrange(
    idncase,
    comp_date,
    desc(is.na(dec_code)),
    dec_code,
    desc(is.na(other_comp)),
    other_comp,
    desc(is.na(nat)),
    nat,
    desc(is.na(lang)),
    lang,
    idnproceeding
  )

rm(proceeding_tbl)
gc()

setDT(cases_from_proceedings)

cases_from_proceedings <-
  cases_from_proceedings[,
    .(
      firstcompdate = first(comp_date),
      finalcompdate = last(comp_date),
      osc_date = first(osc_date),
      firstcourt = first(base_city_code),
      finalcourt = last(base_city_code),
      case_type = first(case_type),
      dec_code = last(dec_code),
      other_comp = last(other_comp),
      absentia = last(absentia),
      nat = last(nat),
      lang = last(lang),
      custody = last(custody),
      firsthearingloc = first(hearing_loc_code),
      lasthearingloc = last(hearing_loc_code),
      ij_code = last(ij_code)
    ),
    by = idncase
  ] |>
  as_tibble()

arrow::write_feather(
  cases_from_proceedings,
  "tmp/cases_from_proceedings.feather"
)
