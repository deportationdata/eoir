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

proc_fix_result <- auto_fix_tab_shifts(proceeding_tbl, proc_shift_finder)
proceeding_tbl <- proc_fix_result$dt

na_vals <- c("", "NA", "N/A", "NULL")

proceeding_tbl <-
  proceeding_tbl |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup tables for validation
lkp_nat <- read_eoir_lookup("inputs_eoir/tblLookupAlienNat.csv")
lkp_lang <- read_eoir_lookup("inputs_eoir/tblLanguage.csv")
lkp_base_city <- read_eoir_lookup("inputs_eoir/tblLookupBaseCity.csv")
lkp_hloc <- read_eoir_lookup("inputs_eoir/tblLookupHloc.csv")
lkp_judge <- read_eoir_lookup("inputs_eoir/tblLookupJudge.csv")
lkp_court_dec <- read_eoir_lookup("inputs_eoir/tblLookupCourtDecision.csv")
lkp_case_type <- read_eoir_lookup("inputs_eoir/tblLookupCaseType.csv")
lkp_custody <- read_eoir_lookup("inputs_eoir/tblLookupCustodyStatus.csv")

# Validate that shift-fixing didn't corrupt key columns
proceeding_tbl |>
  col_vals_not_null(
    IDNPROCEEDING,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    CASE_TYPE, c(lkp_case_type$str_code, "BND", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    CUSTODY, c(lkp_custody$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    ABSENTIA, c("Y", "N", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    TRANSFER_STATUS, c("I", "O", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    DEC_TYPE, c("A", "C", "O", "R", "T", "W", "X", NA),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    NAT, c(lkp_nat$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    LANG, c(lkp_lang$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    BASE_CITY_CODE, c(lkp_base_city$base_city_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    HEARING_LOC_CODE, c(lkp_hloc$hearing_loc_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    SCHEDULED_HEAR_LOC, c(lkp_hloc$hearing_loc_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    PREV_HEARING_LOC, c(lkp_hloc$hearing_loc_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    PREV_HEARING_BASE, c(lkp_base_city$base_city_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    TRANSFER_TO, c(lkp_base_city$base_city_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    IJ_CODE, c(lkp_judge$judge_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    PREV_IJ_CODE, c(lkp_judge$judge_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    DEC_CODE, c(unique(lkp_court_dec$str_dec_code), NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    OTHER_COMP, c(unique(lkp_court_dec$str_dec_code), NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  )

proceeding_tbl <-
  proceeding_tbl |>
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

# Check that date columns parsed without excessive failures
check_date_parse(proceeding_tbl, label = "B_TblProceeding")

# Post-type-convert validation
proceeding_tbl |>
  col_vals_expr(
    expr(is.na(OSC_DATE) | is.na(COMP_DATE) | OSC_DATE <= COMP_DATE),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(is.na(DATE_DETAINED) | is.na(DATE_RELEASED) | DATE_DETAINED <= DATE_RELEASED),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
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
