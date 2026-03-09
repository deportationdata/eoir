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

associated_bond_tbl <- auto_fix_tab_shifts(
  associated_bond_tbl,
  bond_shift_finder
)

associated_bond_tbl <-
  associated_bond_tbl |>
  as_tibble() |>
  clean_eoir_cols() |>
  janitor::clean_names() |>
  mutate(
    idncase = as.integer(idncase),
    bond_comp_date = as.Date(comp_date),
    bond_hear_req_date = as.Date(bond_hear_req_date)
  ) |>
  arrange(idncase, bond_comp_date, idnassocbond)

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
