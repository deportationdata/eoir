library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)
library(collapse)

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

proceeding_tbl <-
  auto_fix_tab_shifts(proceeding_tbl, proc_shift_finder)$dt |>
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
  col_vals_regex(
    IDNPROCEEDING,
    "^\\d+$",
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001),
    na_pass = TRUE
  ) |> # TODO one says FC - need to look into that one
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
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
    ABSENTIA,
    c("Y", "N", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    TRANSFER_STATUS,
    c("C", "T", "V", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    DEC_TYPE,
    c("A", "C", "O", "R", "T", "W", "X", "6", "7", NA), # TODO: look into 6 and 7 values
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
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
    BASE_CITY_CODE,
    c(lkp_base_city$base_city_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    HEARING_LOC_CODE,
    c(lkp_hloc$hearing_loc_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    SCHEDULED_HEAR_LOC,
    c(lkp_hloc$hearing_loc_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    PREV_HEARING_LOC,
    c(lkp_hloc$hearing_loc_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    PREV_HEARING_BASE,
    c(lkp_base_city$base_city_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    TRANSFER_TO,
    c(lkp_base_city$base_city_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.025) # TODO: need to look into this
  ) |>
  col_vals_in_set(
    IJ_CODE,
    c(lkp_judge$judge_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.0025)
  ) |>
  col_vals_in_set(
    PREV_IJ_CODE,
    c(lkp_judge$judge_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.0025)
  ) |>
  col_vals_in_set(
    DEC_CODE,
    c(unique(lkp_court_dec$str_dec_code), NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    OTHER_COMP,
    c(unique(lkp_court_dec$str_dec_code), NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

# proceeding_tbl <-
#   proceeding_tbl |>
#   type_convert(
#     col_types = cols(
#       IDNPROCEEDING = col_integer(),
#       IDNCASE = col_integer(),
#       OSC_DATE = col_date(),
#       INPUT_DATE = col_date(),
#       TRANS_IN_DATE = col_date(),
#       HEARING_DATE = col_date(),
#       COMP_DATE = col_date(),
#       VENUE_CHG_GRANTED = col_date(),
#       DATE_APPEAL_DUE_STATUS = col_date(),
#       AGGRAVATE_FELON = col_logical(),
#       DATE_DETAINED = col_date(),
#       DATE_RELEASED = col_date()
#     ),
#     na = na_vals
#   )

setDT(proceeding_tbl)

proceeding_tbl[, `:=`(
  IDNPROCEEDING = as.integer(IDNPROCEEDING),
  IDNCASE = as.integer(IDNCASE),
  OSC_DATE = as.IDate(OSC_DATE),
  INPUT_DATE = as.IDate(INPUT_DATE),
  TRANS_IN_DATE = as.IDate(TRANS_IN_DATE),
  HEARING_DATE = as.IDate(HEARING_DATE),
  COMP_DATE = as.IDate(COMP_DATE),
  VENUE_CHG_GRANTED = as.IDate(VENUE_CHG_GRANTED),
  DATE_APPEAL_DUE_STATUS = as.IDate(DATE_APPEAL_DUE_STATUS),
  AGGRAVATE_FELON = as.logical(AGGRAVATE_FELON),
  DATE_DETAINED = as.IDate(DATE_DETAINED),
  DATE_RELEASED = as.IDate(DATE_RELEASED)
)]

# TODO:
# Warning messages:
# 1: [594912, 1]: expected no trailing characters, but got 'FC'
# 2: [594912, 8]: expected date like , but got '1000'
# 3: [594912, 22]: expected date like , but got 'N'
# 4: [594912, 24]: expected date like , but got 'ES'
# 5: [594912, 26]: expected date like , but got 'SFR'

# Check that date columns parsed without excessive failures
# chec k_parse(proceeding_tbl)

# Post-type-convert validation
proceeding_tbl |>
  col_vals_expr(
    expr(is.na(OSC_DATE) | is.na(COMP_DATE) | OSC_DATE <= COMP_DATE),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(
      is.na(DATE_DETAINED) |
        is.na(DATE_RELEASED) |
        DATE_DETAINED <= DATE_RELEASED
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

cases_from_proceedings <-
  proceeding_tbl |>
  janitor::clean_names() |>
  rename(
    nta_date = osc_date,
    in_absentia = absentia,
    custody_code = custody,
    case_type_code = case_type,
    judge_code = ij_code
  ) |>
  # clean up in_absentia column which has erroneous values due to csv errors
  # assumes missing values, date errors, and "X", "DEP", and "5" values are not absentia
  mutate(
    in_absentia = case_when(in_absentia == "Y" ~ TRUE, TRUE ~ FALSE)
  ) |>
  # drop rows with missing IDNCASE (creating a case-level dataset)
  # -2 rows
  filter(!is.na(idncase)) |>
  # drop cases with inconsistent case types
  # -314 rows
  filter(n_distinct(case_type_code) == 1, .by = "idncase") |>
  arrange(
    idncase,
    comp_date,
    dec_code,
    other_comp,
    idnproceeding
  )

rm(proceeding_tbl)
gc()

setDT(cases_from_proceedings)

cases_from_proceedings <-
  cases_from_proceedings[,
    .(
      first_proceeding_date = first(comp_date),
      final_completion_date = last(comp_date),
      nta_date = first(nta_date),
      first_court = first(base_city_code),
      final_court = last(base_city_code),
      case_type_code = first(case_type_code),
      dec_code = last(dec_code),
      other_comp = last(other_comp),
      in_absentia = last(in_absentia),
      custody_code = last(custody_code),
      first_hearing_location_code = first(hearing_loc_code),
      last_hearing_location_code = last(hearing_loc_code),
      judge_code = last(judge_code)
    ),
    by = idncase
  ]

# Validate collapsed case-level dataset
cases_from_proceedings |>
  as_tibble() |>
  rows_distinct(idncase) |>
  col_vals_not_null(
    final_completion_date,
    actions = action_levels(warn_at = 0.25, stop_at = 0.5)
  ) |>
  col_vals_not_null(
    nta_date,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_expr(
    expr(
      is.na(nta_date) |
        is.na(final_completion_date) |
        nta_date <= final_completion_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(
      is.na(first_proceeding_date) |
        is.na(final_completion_date) |
        first_proceeding_date <= final_completion_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    final_court,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    judge_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

arrow::write_parquet(
  cases_from_proceedings,
  "tmp/cases_from_proceedings.parquet"
)
