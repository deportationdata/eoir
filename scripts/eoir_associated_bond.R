library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/eoir_utils.R")

associated_bond_tbl <- data.table::fread(
  "inputs_eoir/D_TblAssociatedBond.csv",
  fill = 35
)

associated_bond_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^D_TblAssociatedBond\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(abs(nrow(associated_bond_tbl) - associated_bond_count) < 5)

# Fix mid-row tab shifts (~6 rows with extra tabs near BOND_HEAR_REQ_DATE)
bond_shift_finder <- function(row_dt, n_extra) {
  col_names <- colnames(row_dt)
  date_pat <- "^\\d{4}-\\d{2}-\\d{2}"
  non_date_check <- c("NEW_BOND", "BOND_TYPE", "FILING_METHOD", "FILING_PARTY",
                       "INITIAL_BOND", "REL_CON")
  for (col in non_date_check) {
    if (col %in% col_names) {
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && grepl(date_pat, val)) {
        shift_idx <- which(col_names == col) - n_extra
        if (shift_idx >= 1) return(col_names[shift_idx])
      }
    }
  }
  NA_character_
}

associated_bond_tbl <- auto_fix_tab_shifts(associated_bond_tbl, bond_shift_finder)

associated_bond_tbl <-
  associated_bond_tbl |>
  as_tibble() |>
  clean_string_cols() |>
  drop_overflow_cols() |>
  janitor::clean_names() |>
  mutate(
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
