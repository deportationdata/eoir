library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

associated_bond_tbl <- read_eoir_tsv("inputs_eoir/D_TblAssociatedBond.csv")

# Fix mid-row tab shifts (~6 rows with extra tabs near BOND_HEAR_REQ_DATE)
bond_shift_finder <- make_shift_finder(
  date_cols = c(
    "OSC_DATE",
    "UPDATE_DATE",
    "INPUT_DATE",
    "COMP_DATE",
    "BOND_HEAR_REQ_DATE",
    "DATE_APPEAL_DUE",
    "E_28_DATE",
    "DECISION_DUE_DATE"
  ),
  non_date_cols = c(
    "NEW_BOND",
    "BOND_TYPE",
    "FILING_METHOD",
    "FILING_PARTY",
    "INITIAL_BOND",
    "REL_CON",
    "APPEAL_REVD",
    "APPEAL_NOT_FILED",
    "DEC",
    "INS_TA"
  )
)

bond_fix_result <- auto_fix_tab_shifts(
  associated_bond_tbl,
  bond_shift_finder
)
associated_bond_tbl <- bond_fix_result$dt

associated_bond_tbl <-
  associated_bond_tbl |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup tables for validation
lkp_base_city <- read_eoir_lookup("inputs_eoir/tblLookupBaseCity.csv")
lkp_hloc <- read_eoir_lookup("inputs_eoir/tblLookupHloc.csv")
lkp_judge <- read_eoir_lookup("inputs_eoir/tblLookupJudge.csv")
lkp_court_dec <- read_eoir_lookup("inputs_eoir/tblLookupCourtDecision.csv")
lkp_filing_method <- read_eoir_lookup("inputs_eoir/tblLookupFiling_Method.csv")
lkp_filing_party <- read_eoir_lookup(
  "inputs_eoir/tblLookupFiling_Method_Party.csv"
)

# Validate that shift-fixing didn't corrupt key columns
associated_bond_tbl |>
  col_vals_not_null(
    IDNASSOCBOND,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNPROCEEDING,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNASSOCBOND, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    DEC,
    c(unique(lkp_court_dec$str_dec_code), "I", "R", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    BOND_TYPE,
    c("BB", "BD", "SB", NA),
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
    IJ_CODE,
    c(lkp_judge$judge_code, NA),
    # set higher threshold because there are many that don't match the lookup
    # TODO: drop these codes in future
    actions = action_levels(warn_at = 0.001, stop_at = 0.005)
  ) |>
  col_vals_in_set(
    FILING_METHOD,
    c(lkp_filing_method$str_filing_method_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    FILING_PARTY,
    c(lkp_filing_party$str_filing_party_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    INITIAL_BOND,
    "^\\d*(\\.\\d+)?$",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    NEW_BOND,
    "^\\d*(\\.\\d+)?$",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

associated_bond_tbl <- type_convert(
  associated_bond_tbl,
  col_types = cols(
    IDNASSOCBOND = col_integer(),
    IDNPROCEEDING = col_integer(),
    IDNCASE = col_integer(),
    OSC_DATE = col_date(),
    UPDATE_DATE = col_date(),
    INPUT_DATE = col_date(),
    COMP_DATE = col_date(),
    BOND_HEAR_REQ_DATE = col_date(),
    DATE_APPEAL_DUE = col_date(),
    E_28_DATE = col_date(),
    DECISION_DUE_DATE = col_date(),
    INITIAL_BOND = col_double(),
    NEW_BOND = col_double()
  ),
  na = na_vals
)

check_parse(associated_bond_tbl)

associated_bond_tbl <-
  associated_bond_tbl |>
  janitor::clean_names() |>
  rename(
    bond_completion_date = comp_date,
    bond_hearing_request_date = bond_hear_req_date,
    bond_decision = dec,
    initial_bond_amount = initial_bond,
    new_bond_amount = new_bond
  ) |>
  arrange(idncase, bond_completion_date, idnassocbond)

# Post-transform validation
associated_bond_tbl |>
  col_vals_gte(initial_bond_amount, 0, na_pass = TRUE) |>
  col_vals_gte(new_bond_amount, 0, na_pass = TRUE) |>
  col_vals_expr(
    expr(
      is.na(bond_hearing_request_date) |
        is.na(bond_completion_date) |
        bond_hearing_request_date <= bond_completion_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

setDT(associated_bond_tbl)

associated_bond_by_case <-
  associated_bond_tbl[,
    .(
      bond_completion_date = last(bond_completion_date),
      bond_hearing_request_date = last(bond_hearing_request_date),
      bond_decision = last(bond_decision),
      initial_bond_amount = last(initial_bond_amount),
      new_bond_amount = last(new_bond_amount)
    ),
    by = idncase
  ] |>
  mutate(
    bond_decision = recode(
      bond_decision,
      G = "AMELIORATION GRANTED",
      W = "BOND REQUEST WITHDRAWN",
      D = "AMELIORATION DENIED-NO JURISDICTION",
      I = "BOND AMOUNT INCREASED",
      E = "AMELIORATION DENIED",
      O = "OTHER",
      F = "FLORES - RELEASE",
      L = "FLORES - NO RELEASE",
      A = "BOND DENIED-MOOT",
      C = "BOND GRANTED-AMOUNT DECREASED",
      J = "BOND DENIED-NO JURISDICTION",
      N = "BOND DENIED- NO CHANGE (NO BOND SET BY DHS)",
      S = "BOND DENIED- NO CHANGE (DHS BOND AMOUNT UNCHANGED)",
      R = "BOND GRANTED-OWN RECOGNIZANCE",
      .default = NA_character_
    )
  )

arrow::write_parquet(
  associated_bond_by_case,
  "tmp/associated_bond_cases.parquet"
)
