library(tidyverse)
library(tidylog)
library(data.table)

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

# Validate that shift-fixing didn't corrupt key columns
associated_bond_tbl |>
  col_vals_not_null(IDNASSOCBOND) |>
  col_vals_not_null(IDNPROCEEDING) |>
  col_vals_not_null(IDNCASE) |>
  col_vals_regex(IDNASSOCBOND, "^\\d+$") |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$") |>
  col_vals_regex(IDNCASE, "^\\d+$") |>
  col_vals_in_set(
    DEC,
    c("G", "W", "D", "I", "E", "O", "F", "L", "A", "C", "J", "N", "S", "R", NA)
  ) |>
  col_vals_in_set(BOND_TYPE, c("B", "D", "F", "N", "O", "R", "S", NA)) |>
  col_vals_in_set(BASE_CITY_CODE, c(lkp_base_city$base_city_code, NA)) |>
  col_vals_in_set(HEARING_LOC_CODE, c(lkp_hloc$hearing_loc_code, NA)) |>
  col_vals_in_set(IJ_CODE, c(lkp_judge$judge_code, NA)) |>
  col_vals_regex(INITIAL_BOND, "^\\d+(\\.\\d+)?$", na_pass = TRUE) |>
  col_vals_regex(NEW_BOND, "^\\d+(\\.\\d+)?$", na_pass = TRUE)

associated_bond_tbl <-
  associated_bond_tbl |>
  janitor::clean_names() |>
  mutate(
    idncase = as.integer(idncase),
    bond_comp_date = as.Date(comp_date),
    bond_hear_req_date = as.Date(bond_hear_req_date),
    initial_bond = as.numeric(initial_bond),
    new_bond = as.numeric(new_bond)
  ) |>
  arrange(idncase, bond_comp_date, idnassocbond)

# Post-transform validation
associated_bond_tbl |>
  col_vals_gte(initial_bond, 0, na_pass = TRUE) |>
  col_vals_gte(new_bond, 0, na_pass = TRUE) |>
  col_vals_gt(idncase, 0) |>
  col_vals_expr(
    expr(
      is.na(bond_hear_req_date) |
        is.na(bond_comp_date) |
        bond_hear_req_date <= bond_comp_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  )

setDT(associated_bond_tbl)

associated_bond_by_case <-
  associated_bond_tbl[,
    .(
      lastbond_comp_date = last(bond_comp_date),
      lastbond_hear_req_date = last(bond_hear_req_date),
      lastdec = last(dec),
      lastinitial_bond = last(initial_bond),
      lastnew_bond = last(new_bond)
    ),
    by = idncase
  ] |>
  as_tibble() |>
  mutate(
    lastdec = recode(
      lastdec,
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

arrow::write_feather(
  associated_bond_by_case,
  "tmp/associated_bond_cases.feather"
)
