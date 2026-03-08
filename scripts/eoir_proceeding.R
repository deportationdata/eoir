library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/eoir_utils.R")

proceeding_col_types <- c(
  IDNPROCEEDING = "integer",
  IDNCASE = "integer",
  OSC_DATE = "POSIXct",
  INPUT_DATE = "POSIXct",
  BASE_CITY_CODE = "character",
  HEARING_LOC_CODE = "character",
  IJ_CODE = "character",
  TRANS_IN_DATE = "POSIXct",
  PREV_HEARING_LOC = "character",
  PREV_HEARING_BASE = "character",
  PREV_IJ_CODE = "character",
  TRANS_NBR = "character",
  HEARING_DATE = "POSIXct",
  HEARING_TIME = "character",
  DEC_TYPE = "character",
  DEC_CODE = "character",
  DEPORTED_1 = "character",
  DEPORTED_2 = "character",
  OTHER_COMP = "character",
  APPEAL_RSVD = "character",
  APPEAL_NOT_FILED = "character",
  COMP_DATE = "POSIXct",
  ABSENTIA = "character",
  VENUE_CHG_GRANTED = "POSIXct",
  TRANSFER_TO = "character",
  DATE_APPEAL_DUE_STATUS = "POSIXct",
  TRANSFER_STATUS = "character",
  CUSTODY = "character",
  CASE_TYPE = "character",
  NAT = "character",
  LANG = "character",
  SCHEDULED_HEAR_LOC = "character",
  CORRECTIONAL_FAC = "character",
  CRIM_IND = "character",
  IHP = "character",
  AGGRAVATE_FELON = "logical", # bit → logical
  DATE_DETAINED = "POSIXct",
  DATE_RELEASED = "POSIXct"
)

proceeding_tbl <-
  data.table::fread(
    "inputs_eoir/B_TblProceeding.csv",
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = c("", "NA", "N/A", "NULL"),
    colClasses = proceeding_col_types,
    fill = 40,
    showProgress = FALSE
  )

proceeding_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^B_TblProceeding\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(abs(nrow(proceeding_tbl) - proceeding_count) < 5)

# Fix mid-row tab shifts (~71 rows with extra tabs before COMP_DATE/APPEAL area)
proc_shift_finder <- function(row_dt, n_extra) {
  col_names <- colnames(row_dt)
  date_pat <- "^\\d{4}-\\d{2}-\\d{2}"
  date_cols <- c("OSC_DATE", "INPUT_DATE", "TRANS_IN_DATE", "HEARING_DATE",
                 "COMP_DATE", "VENUE_CHG_GRANTED", "DATE_APPEAL_DUE_STATUS",
                 "DATE_DETAINED", "DATE_RELEASED")
  non_date_cols <- c("CUSTODY", "CASE_TYPE", "NAT", "LANG",
                     "ABSENTIA", "TRANSFER_STATUS")
  violations <- integer(0)
  for (col in date_cols) {
    if (!col %in% col_names) next
    val <- trimws(as.character(row_dt[[col]]))
    if (!is.na(val) && nchar(val) > 0 && !grepl(date_pat, val))
      violations <- c(violations, which(col_names == col))
  }
  for (col in non_date_cols) {
    if (!col %in% col_names) next
    val <- trimws(as.character(row_dt[[col]]))
    if (!is.na(val) && grepl(date_pat, val))
      violations <- c(violations, which(col_names == col))
  }
  if (!length(violations)) return(NA_character_)
  shift_idx <- min(violations) - n_extra
  if (shift_idx >= 1) col_names[shift_idx] else NA_character_
}

proceeding_tbl <- auto_fix_tab_shifts(proceeding_tbl, proc_shift_finder)

proceeding_tbl <-
  proceeding_tbl |>
  as_tibble() |>
  clean_string_cols() |>
  drop_overflow_cols()

cases_from_proceedings <-
  proceeding_tbl |>
  janitor::clean_names() |>
  # convert OSC_DATE and COMP_DATE to date to remove unused time information
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

cases_from_proceedings <-
  cases_from_proceedings |>
  as_tibble()

arrow::write_feather(
  cases_from_proceedings,
  "tmp/cases_from_proceedings.feather"
)
