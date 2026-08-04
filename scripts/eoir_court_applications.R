library(tidyverse)
library(duckplyr)
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

# The validations, fast_convert() and the renaming below are all R-only work —
# pointblank has no duckplyr backend, and col_vals_expr() evaluates an R
# expression. Leaving duckplyr's methods in place makes every internal dplyr
# call round-trip the whole frame through DuckDB for nothing: measured on
# 1.2M x 40, that chain costs 47.3s and +3474MB with the methods overwritten
# versus 20.1s and +538MB without. At full scale it was the pipeline's memory
# peak, 61.8GB of 64.3GB.
duckplyr::methods_restore()

# Load lookup tables for validation
lkp_appln <- read_eoir_lookup("inputs_eoir/tblLookUp_Appln.csv")
lkp_appl_dec <- read_eoir_lookup("inputs_eoir/tblLookupCourtAppDecisions.csv")

log_step("applications: pre-transform validation")
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

log_step("applications: type conversion")
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

# Substantive decisions outrank administrative dispositions
# when applications of the same type are received the same day
DEC_PRIORITY <- c(
  "F", # FULL GRANT
  "G", # GRANT
  "D", # DENY
  "C", # CONDITIONAL GRANT
  "I", # IN COURT STIPULATED GRANT
  "L", # GRANT WCAT
  "P", # PAPER STIPULATED GRANT
  "S", # ADMIN CLOSURE
  "A", # ABANDONMENT
  "R", # RESERVED
  "O", # OTHER
  "T", # COV/TRANSFER
  "W", # WITHDRAWN
  "M" # NOT ADJUDICATED
)


# Back to duckplyr: the collapse below runs inside DuckDB.
duckplyr::methods_overwrite()

log_step("applications: rank and sort")
court_applications_tbl <- court_applications_tbl |>
  mutate(
    dec_rank = coalesce(match(appl_dec, rev(DEC_PRIORITY)), 0L),
    appl_recd_day = as.Date(appl_recd_date)
  ) |>
  # na.last = FALSE: rows with no appl_recd_day sort first within each case,
  # so they're never mistaken for the most recent decision by last() below
  arrange(
    idncase,
    desc(is.na(appl_recd_day)),
    appl_recd_day,
    dec_rank,
    appl_recd_date,
    idnproceedingappln
  ) |>
  # sort order frozen into a column for the collapse below to order by; it
  # also carries the desc() term, which order_by= alone cannot express
  mutate(row_order = row_number())

#' Most recent decision for a single application type, one row per case.
#'
#' Filtering to one application type first, rather than subsetting inside one
#' grouped `last(appl_dec[appl_code %in% ...])`, is what makes this cheap: the
#' subset form cost about 15 minutes on the full 16.1M-row table.
#'
#' A case with no application of a given type has no row here at all, so the
#' left_join below leaves NA — the same answer the subset form gave when
#' handed the empty vector it used to produce.
last_decision_for <- function(appl_code_value, column_name) {
  court_applications_tbl |>
    filter(appl_code %in% appl_code_value) |>
    summarise(
      "{column_name}" := dplyr::last(appl_dec, order_by = row_order),
      .by = idncase
    )
}

log_step("applications: six decision joins")
court_applications_by_case <-
  court_applications_tbl |>
  distinct(idncase) |>
  # dropped here rather than after the joins so NA cases never enter them
  filter(!is.na(idncase)) |>
  left_join(last_decision_for("ASYL", "asylum_decision_last"), by = "idncase") |>
  left_join(
    last_decision_for("ASYW", "withholding_decision_last"),
    by = "idncase"
  ) |>
  left_join(last_decision_for("WCAT", "cat_decision_last"), by = "idncase") |>
  left_join(
    last_decision_for("245", "adjustment_decision_last"),
    by = "idncase"
  ) |>
  left_join(
    last_decision_for("42B", "non_lpr_cancellation_decision_last"),
    by = "idncase"
  ) |>
  left_join(
    last_decision_for("42A", "lpr_cancellation_decision_last"),
    by = "idncase"
  ) |>
  arrange(idncase) |>
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

log_step("applications: validate collapsed")
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

log_step("applications: write parquet")
arrow::write_parquet(
  court_applications_by_case,
  "tmp/court_applications_cases.parquet",
  compression = "ZSTD"
)
