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
  # Decision order defines the first and last appeal in a case.
  # Pending appeals (no BIA decision date) sort last.
  # Ties are broken by filing date, then appeal ID.
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

# --- Remand codes ---
# BIA decisions that send the case back to the immigration court:
# REM (Remand) and BCR (Background Check Remand).
# The "Circuit Court Remand" appeal type is a different event and is not counted.
remand_codes <- c("REM", "BCR")

missing_remand_codes <- setdiff(remand_codes, lkp_bia_dec$str_code)
if (length(missing_remand_codes) > 0) {
  stop(
    "Remand codes missing from tblLookupBIADecision: ",
    paste(missing_remand_codes, collapse = ", ")
  )
}

# --- Pending appeals ---
# An appeal is pending if it was filed but has neither a BIA decision date nor a BIA decision code.
appeals_tbl[,
  pending := !is.na(appeal_filed_date) &
    is.na(bia_decision_date) &
    is.na(bia_decision)
]

# Pending ages are measured as of the extract date, from the zip's folder name
# (e.g. "EOIR Case Data 2026-1001"), which the workflow saves to inputs_eoir/zip_contents.txt.
extract_folders <- readLines("inputs_eoir/zip_contents.txt") |>
  str_extract("^EOIR Case Data \\d{4}-\\d{4}(?=/)") |>
  na.omit() |>
  unique()

if (length(extract_folders) != 1) {
  stop(
    "Expected one 'EOIR Case Data YYYY-MMDD' folder in the zip, found: ",
    paste(extract_folders, collapse = ", ")
  )
}

extract_date <- as.Date(str_extract(extract_folders, "\\d{4}-\\d{4}$"), format = "%Y-%m%d")

if (is.na(extract_date)) {
  stop("Could not read the extract date from folder name: ", extract_folders)
}

message(sprintf("Pending appeal ages computed as of the extract date, %s", format(extract_date)))

# --- Case-level summary across all appeals ---
appeals_summary <-
  appeals_tbl[,
    .(
      appeal_count = .N,
      ever_remanded = any(bia_decision %in% remand_codes),
      has_pending_appeal = any(pending)
    ),
    by = .(idncase)
  ]

# Age of the oldest pending appeal.
# If a case has more than one pending appeal, the earliest-filed one is used.
pending_by_case <-
  appeals_tbl[
    pending == TRUE,
    .(oldest_pending_filed_date = min(appeal_filed_date)),
    by = .(idncase)
  ]

pending_by_case[,
  pending_age_days := as.numeric(
    extract_date - as.Date(oldest_pending_filed_date, tz = "UTC")
  )
][,
  oldest_pending_filed_date := NULL
]

# --- First and last appeal ---
# appeals_tbl is in decision order with pending appeals last, so:
# - _first is the earliest-decided appeal, or the earliest-filed pending appeal if none is decided.
# - _last is the most recently decided appeal, or the most recently filed pending appeal if any.
# Each set of columns describes a single appeal.
appeals_first_last <-
  appeals_tbl[,
    .(
      bia_decision_code_first = first(bia_decision),
      bia_decision_type_code_first = first(bia_decision_type_code),
      appeal_type_code_first = first(appeal_type),
      appeal_filed_by_code_first = first(appeal_filed_by_code),
      custody_at_appeal_code_first = first(custody_at_appeal_code),
      e27_date_first = first(e27_date),
      bia_decision_date_first = first(bia_decision_date),
      appeal_filed_date_first = first(appeal_filed_date),

      bia_decision_code_last = last(bia_decision),
      bia_decision_type_code_last = last(bia_decision_type_code),
      appeal_type_code_last = last(appeal_type),
      appeal_filed_by_code_last = last(appeal_filed_by_code),
      custody_at_appeal_code_last = last(custody_at_appeal_code),
      e27_date_last = last(e27_date),
      bia_decision_date_last = last(bia_decision_date),
      appeal_filed_date_last = last(appeal_filed_date)
    ),
    by = .(idncase)
  ]

# Combine into one row per case with any appeal:
# - appeals_summary: appeal_count, ever_remanded, has_pending_appeal (every case)
# - pending_by_case: pending_age_days (only cases with a pending appeal; NA otherwise)
# - appeals_first_last: the _first and _last appeal columns (every case)
appeals_by_case <-
  appeals_summary |>
  merge(pending_by_case, by = "idncase", all.x = TRUE) |>
  merge(appeals_first_last, by = "idncase", all.x = TRUE)

rm(appeals_summary, pending_by_case, appeals_first_last)

appeals_by_case |>
  as_tibble() |>
  rows_distinct(idncase) |>
  col_vals_gte(appeal_count, 1) |>
  col_vals_not_null(c(ever_remanded, has_pending_appeal)) |>
  col_vals_gte(pending_age_days, 0, na_pass = TRUE) |>
  # pending age is present exactly when the case has a pending appeal
  col_vals_expr(expr(is.na(pending_age_days) == !has_pending_appeal)) |>
  col_vals_in_set(
    c(custody_at_appeal_code_first, custody_at_appeal_code_last),
    c("D", "N", "R", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # the first decision cannot come after the last
  col_vals_expr(
    expr(
      is.na(bia_decision_date_first) |
        is.na(bia_decision_date_last) |
        bia_decision_date_first <= bia_decision_date_last
    )
  ) |>
  invisible()

arrow::write_parquet(
  appeals_by_case,
  "tmp/appeals_cases.parquet"
)

# --- Appeal-level output ---
# One row per appeal or motion, saved to data/ alongside cases.parquet.
# Not filtered to the cases in cases.parquet; join on idncase to restrict.
# Rows are in filing order within each case (by BIA decision date when there is no filing date).
appeals_out <-
  appeals_tbl[order(
    idncase,
    fcoalesce(appeal_filed_date, bia_decision_date),
    idn_appeal
  ), .(
    idn_appeal,
    idncase,
    idn_proceeding,
    appeal_category_code = str_appeal_category,
    appeal_type_code = appeal_type,
    appeal_filed_by_code,
    appeal_filed_date,
    e27_date,
    bia_decision_code = bia_decision,
    bia_decision_type_code,
    bia_decision_date,
    pending,
    custody_at_appeal_code,
    case_type_code = str_case_type,
    language_code = str_lang,
    nationality_code = str_nat,
    proceeding_ihp_code = str_proceeding_ihp,
    probono = str_probono
  )]

appeals_out |>
  as_tibble() |>
  rows_distinct(idn_appeal) |>
  col_vals_not_null(idn_appeal) |>
  invisible()

arrow::write_parquet(appeals_out, "data/appeals.parquet")
