library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

# Fix mid-row tab shifts
appeal_shift_finder <- make_shift_finder(
  date_cols = c("datAppealFiled", "datAttorneyE27", "datBIADecision"),
  non_date_cols = c(
    "strAppealCategory",
    "strAppealType",
    "strFiledBy",
    "strBIADecision",
    "strBIADecisionType",
    "strCaseType",
    "strLang",
    "strNat",
    "strCustody",
    "strProbono"
  )
)

appeals_raw <- read_eoir_tsv("inputs_eoir/tblAppeal.csv")

appeal_fix_result <- auto_fix_tab_shifts(appeals_raw, appeal_shift_finder)

appeals_tbl <-
  appeal_fix_result$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup tables for validation
lkp_bia_dec <- read_eoir_lookup("inputs_eoir/tblLookupBIADecision.csv")
lkp_appeal_type <- read_eoir_lookup("inputs_eoir/tbllookupAppealType.csv")
lkp_nat <- read_eoir_lookup("inputs_eoir/tblLookupAlienNat.csv")
lkp_lang <- read_eoir_lookup("inputs_eoir/tblLanguage.csv")
lkp_case_type <- read_eoir_lookup("inputs_eoir/tblLookupCaseType.csv")
lkp_bia_dec_type <- read_eoir_lookup("inputs_eoir/tblLookupBIADecisionType.csv")

# Validate before transforms
appeals_tbl |>
  col_vals_not_null(
    idncase,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(idncase, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(idnAppeal, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(
    idnProceeding,
    "^\\d+$",
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001),
    na_pass = TRUE
  ) |>
  col_vals_in_set(strAppealCategory, c("DD", "IJ", "MA", "ij", NA)) |>
  col_vals_in_set(
    strBIADecision,
    c(lkp_bia_dec$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    strBIADecisionType,
    c(lkp_bia_dec_type$str_code, NA),
    actions = action_levels(warn_at = 0.00001, stop_at = 0.0001)
  ) |>
  col_vals_in_set(
    strAppealType,
    c(lkp_appeal_type$str_appl_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    strCaseType,
    c(lkp_case_type$str_code, "BND", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    strCustody,
    c("D", "N", "R", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    strNat,
    c(lkp_nat$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    strLang,
    c(lkp_lang$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    strProbono,
    c("Y", "N", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

appeals_tbl <- fast_convert(
  appeals_tbl,
  list(
    idnAppeal = "integer",
    idnProceeding = "integer",
    idncase = "integer",
    datAppealFiled = "datetime",
    datBIADecision = "datetime",
    datAttorneyE27 = "datetime"
  )
)

appeals_tbl <-
  appeals_tbl |>
  janitor::clean_names() |>
  rename(
    appeal_filed_date = dat_appeal_filed,
    bia_decision_date = dat_bia_decision,
    e27_date = dat_attorney_e27,
    bia_decision = str_bia_decision,
    bia_decision_type_code = str_bia_decision_type,
    appeal_type = str_appeal_type,
    appeal_filed_by_code = str_filed_by,
    custody_at_appeal_code = str_custody
  ) |>
  arrange(idncase, bia_decision_date, appeal_filed_date, idn_appeal)

# Validate date ordering (appeal filed before decision)
appeals_tbl |>
  col_vals_expr(
    expr(
      is.na(appeal_filed_date) |
        is.na(bia_decision_date) |
        appeal_filed_date <= bia_decision_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

setDT(appeals_tbl)

appeals_by_case <-
  appeals_tbl[,
    .(
      bia_decision = last(bia_decision),
      bia_decision_type_code = last(bia_decision_type_code),
      # appeal_category = last(appeal_category),
      appeal_type = last(appeal_type),
      appeal_filed_by_code = last(appeal_filed_by_code),
      custody_at_appeal_code = last(custody_at_appeal_code),
      e27_date = last(e27_date),
      bia_decision_date = last(bia_decision_date),
      appeal_filed_date = last(appeal_filed_date)
      # pending_appeal = any(is.na(bia_decision_date) & !is.na(appeal_filed_date))
    ),
    by = .(idncase)
  ]

arrow::write_parquet(
  appeals_by_case,
  "tmp/appeals_cases.parquet"
)
