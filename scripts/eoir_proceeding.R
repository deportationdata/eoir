library(tidyverse)
library(duckplyr)
library(pointblank)

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

# The validations, fast_convert() and the renaming below are all R-only work —
# pointblank has no duckplyr backend, and col_vals_expr() evaluates an R
# expression. Leaving duckplyr's methods in place makes every internal dplyr
# call round-trip the whole frame through DuckDB for nothing: measured on
# 1.2M x 40, that chain costs 47.3s and +3474MB with the methods overwritten
# versus 20.1s and +538MB without. At full scale it was the pipeline's memory
# peak, 61.8GB of 64.3GB.
duckplyr::methods_restore()

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
log_step("proceeding: pre-transform validation")
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
    DEPORTED_1,
    c(lkp_nat$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    DEPORTED_2,
    c(lkp_nat$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    expr(is.na(DEPORTED_2) | !is.na(DEPORTED_1)),
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(
      is.na(DEPORTED_1) | is.na(DEPORTED_2) | DEPORTED_1 != DEPORTED_2
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(is.na(DEC_CODE) | DEC_CODE != "D" | !is.na(DEPORTED_1)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
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

log_step("proceeding: type conversion")
proceeding_tbl <- proceeding_tbl |>
  mutate(
    IDNPROCEEDING = as.integer(IDNPROCEEDING),
    IDNCASE = as.integer(IDNCASE),
    OSC_DATE = as.Date(OSC_DATE),
    INPUT_DATE = as.Date(INPUT_DATE),
    TRANS_IN_DATE = as.Date(TRANS_IN_DATE),
    HEARING_DATE = as.Date(HEARING_DATE),
    COMP_DATE = as.Date(COMP_DATE),
    VENUE_CHG_GRANTED = as.Date(VENUE_CHG_GRANTED),
    DATE_APPEAL_DUE_STATUS = as.Date(DATE_APPEAL_DUE_STATUS),
    AGGRAVATE_FELON = as.logical(AGGRAVATE_FELON),
    DATE_DETAINED = as.Date(DATE_DETAINED),
    DATE_RELEASED = as.Date(DATE_RELEASED)
  )

# TODO:
# Warning messages:
# 1: [594912, 1]: expected no trailing characters, but got 'FC'
# 2: [594912, 8]: expected date like , but got '1000'
# 3: [594912, 22]: expected date like , but got 'N'
# 4: [594912, 24]: expected date like , but got 'ES'
# 5: [594912, 26]: expected date like , but got 'SFR'

# Check that date columns parsed without excessive failures
# chec k_parse(proceeding_tbl)

log_step("proceeding: post-transform validation")
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

log_step("proceeding: select and recode")

# Back to duckplyr: the collapse below runs inside DuckDB.
duckplyr::methods_overwrite()

cases_from_proceedings <-
  proceeding_tbl |>
  janitor::clean_names() |>
  rename(
    nta_date = osc_date,
    in_absentia = absentia,
    case_type_code = case_type,
    judge_code = ij_code
  ) |>
  # Only these 13 of the table's 40 columns are sorted on or collapsed below.
  # Carrying the other 27 through the sort roughly triples what DuckDB has to
  # hold, on the largest table in the pipeline.
  select(
    idncase,
    idnproceeding,
    comp_date,
    nta_date,
    base_city_code,
    case_type_code,
    dec_code,
    other_comp,
    in_absentia,
    hearing_loc_code,
    judge_code,
    deported_1,
    deported_2
  ) |>
  # clean up in_absentia column which has erroneous values due to csv errors
  # assumes missing values, date errors, and "X", "DEP", and "5" values are not absentia
  mutate(
    in_absentia = !is.na(in_absentia) & in_absentia == "Y"
  ) |>
  # drop rows with missing IDNCASE (creating a case-level dataset)
  # -2 rows
  filter(!is.na(idncase))

# The 40-column source frame is no longer needed; release it before the sort.
rm(proceeding_tbl)
gc()

log_step("proceeding: drop inconsistent case types, sort")
# Cases whose proceedings disagree on case type are dropped (-314 rows).
consistent_case_types <-
  cases_from_proceedings |>
  summarise(n_case_types = n_distinct(case_type_code), .by = idncase) |>
  filter(n_case_types == 1) |>
  select(idncase)

cases_from_proceedings <-
  cases_from_proceedings |>
  inner_join(consistent_case_types, by = "idncase") |>
  arrange(
    idncase,
    comp_date,
    dec_code,
    other_comp,
    idnproceeding
  ) |>
  # Freeze the sort order into a column. A grouped aggregate is not promised
  # the rows in input order, so first()/last()/nth() must be told the order
  # explicitly; without it the collapse below returns the wrong proceeding for
  # a handful of cases, and a different handful on each run.
  mutate(row_order = row_number())

rm(consistent_case_types)

log_step("proceeding: collapse to one row per case")
cases_from_proceedings <-
  cases_from_proceedings |>
  summarise(
    final_completion_date = last(comp_date, order_by = row_order),
    nta_date = first(nta_date, order_by = row_order),
    first_court_code = first(base_city_code, order_by = row_order),
    final_court_code = last(base_city_code, order_by = row_order),
    case_type_code = first(case_type_code, order_by = row_order),
    dec_code = last(dec_code, order_by = row_order),
    other_comp = last(other_comp, order_by = row_order),
    in_absentia = last(in_absentia, order_by = row_order),
    first_hearing_location_code = first(hearing_loc_code, order_by = row_order),
    last_hearing_location_code = last(hearing_loc_code, order_by = row_order),
    first_judge_code = first(judge_code, order_by = row_order),
    last_judge_code = last(judge_code, order_by = row_order),
    deported_1_code = last(deported_1, order_by = row_order),
    deported_2_code = last(deported_2, order_by = row_order),
    .by = idncase
  ) |>
  arrange(idncase)

log_step("proceeding: validate collapsed dataset")
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
  col_vals_not_null(
    first_court_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    final_court_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    first_judge_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    last_judge_code,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

log_step("proceeding: write parquet")
arrow::write_parquet(
  cases_from_proceedings,
  "tmp/cases_from_proceedings.parquet",
  compression = "ZSTD"
)
