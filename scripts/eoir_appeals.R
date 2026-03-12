library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/utilities.R")

appeals_tbl <-
  read_eoir_tsv("inputs_eoir/tblAppeal.csv") |>
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
  )

appeals_tbl <-
  appeals_tbl |>
  janitor::clean_names() |>
  mutate(
    idncase = as.integer(idncase),
    datappealfiled = as.Date(dat_appeal_filed),
    datbiadec = as.Date(dat_bia_decision),
    dat_attorney_e27 = as.POSIXct(dat_attorney_e27)
  ) |>
  arrange(idncase, datbiadec, datappealfiled, idn_appeal)

# Validate date ordering (appeal filed before decision)
appeals_tbl |>
  col_vals_expr(
    expr(
      is.na(datappealfiled) | is.na(datbiadec) | datappealfiled <= datbiadec
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  )

setDT(appeals_tbl)

appeals_by_case <-
  appeals_tbl[,
    .(
      lastbiadecision = last(str_bia_decision),
      lastbiadecisiontype = last(str_bia_decision_type),
      lastappealcategory = last(str_appeal_category),
      lastappealtype = last(str_appeal_type),
      lastbiafiledby = last(str_filed_by),
      lastbiacustody = last(str_custody),
      last_e_27date = last(dat_attorney_e27),
      datbiadec = last(datbiadec),
      datappealfiled = last(datappealfiled),
      pendingappeal = case_when(
        any(is.na(datbiadec) & !is.na(datappealfiled)) ~ TRUE,
        TRUE ~ FALSE
      )
    ),
    by = .(idncase)
  ] |>
  as_tibble()

arrow::write_feather(
  appeals_by_case,
  "tmp/appeals_cases.feather"
)
