library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

court_appln_shift_finder <- make_shift_finder(
  date_cols = c("APPL_RECD_DATE"),
  non_date_cols = c("APPL_CODE", "APPL_DEC")
)

court_applications_raw <- read_eoir_tsv("inputs_eoir/tbl_Court_Appln.csv")

appln_fix_result <- auto_fix_tab_shifts(
  court_applications_raw,
  court_appln_shift_finder
)

court_applications_tbl <-
  appln_fix_result$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup tables for validation
lkp_appln <- read_eoir_lookup("inputs_eoir/tblLookUp_Appln.csv")
lkp_appl_dec <- read_eoir_lookup("inputs_eoir/tblLookupCourtAppDecisions.csv")

# Validate before transforms
court_applications_tbl |>
  col_vals_not_null(
    IDNPROCEEDINGAPPLN,
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
  col_vals_regex(IDNPROCEEDINGAPPLN, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    APPL_CODE,
    c(lkp_appln$strcode, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_in_set(
    APPL_DEC,
    c(lkp_appl_dec$str_court_appln_dec_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_regex(
    APPL_RECD_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

court_applications_tbl <- fast_convert(
  court_applications_tbl,
  list(
    IDNPROCEEDINGAPPLN = "integer",
    IDNPROCEEDING = "integer",
    IDNCASE = "integer",
    APPL_RECD_DATE = "datetime"
  )
)

court_applications_tbl <-
  court_applications_tbl |>
  janitor::clean_names()

setDT(court_applications_tbl)

setorder(
  court_applications_tbl,
  idncase,
  appl_recd_date,
  idnproceedingappln,
  na.last = FALSE
)

# dplyr::last is namespaced because data.table masks it; dplyr's version
# returns NA for cases with no application of the given type
court_applications_by_case <-
  court_applications_tbl[,
    .(
      asylum_decision_last = dplyr::last(appl_dec[appl_code %in% "ASYL"]),
      withholding_decision_last = dplyr::last(appl_dec[appl_code %in% "ASYW"]),
      cat_decision_last = dplyr::last(appl_dec[appl_code %in% "WCAT"]),
      adjustment_decision_last = dplyr::last(appl_dec[appl_code %in% "245"]),
      non_lpr_cancellation_decision_last = dplyr::last(appl_dec[
        appl_code %in% "42B"
      ]),
      lpr_cancellation_decision_last = dplyr::last(appl_dec[
        appl_code %in% "42A"
      ])
      # any_relief_decision = dplyr::last(
      #   appl_dec[!is.na(appl_code) & appl_code != "VD"]
      # )
    ),
    by = idncase
  ] |>
  filter(!is.na(idncase)) |>
  # Recode decision codes to human-readable labels (tblLookupCourtAppDecisions)
  mutate(
    across(
      ends_with("_decision_last"),
      \(x) {
        recode(
          x,
          A = "ABANDONMENT",
          C = "CONDITIONAL GRANT",
          D = "DENY",
          F = "FULL GRANT",
          G = "GRANT",
          I = "IN COURT STIPULATED GRANT",
          L = "GRANT WCAT",
          M = "NOT ADJUDICATED",
          O = "OTHER",
          P = "PAPER STIPULATED GRANT",
          R = "RESERVED",
          S = "ADMIN CLOSURE",
          T = "COV/TRANSFER",
          W = "WITHDRAWN",
          .default = x
        )
      }
    )
  )

court_applications_by_case |>
  rows_distinct(idncase) |>
  # decisions must be valid decision labels (or NA when no such application);
  # this also catches any codes the recode above failed to resolve
  col_vals_in_set(
    c(
      asylum_decision_last,
      withholding_decision_last,
      cat_decision_last,
      adjustment_decision_last,
      non_lpr_cancellation_decision_last,
      lpr_cancellation_decision_last
      # any_relief_decision
    ),
    c(lkp_appl_dec$str_court_appln_dec_desc, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

arrow::write_parquet(
  court_applications_by_case,
  "tmp/court_applications_cases.parquet"
)
