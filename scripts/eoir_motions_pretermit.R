library(tidyverse)
library(pointblank)

source("scripts/utilities.R")

# Minimal alternative to eoir_motions.R: for each case, keep only the most
# recently received motion to pretermit (REC_TYPE == "PM") and record its
# received date, completion date, and decision code. Output columns use the
# pretermit_motion_ prefix so they sit alongside (and can be checked against)
# the motion_to_pretermit_ columns from the full motions build.
#
# Motivating question: how many of the 30k+ removal orders issued by IJs after
# pretermission pursuant to an ACA have been appealed to the BIA?

# Motion decision codes have no dedicated lookup in the FOIA release. G/D/O/W
# match the other EOIR decision fields (grant, deny, other, withdrawn);
# M = "Not Adjudicated" / moot (tblLookupCourtAppDecisions, 2017);
# J = "Jurisdiction Transferred to the BIA" (tblLookupCourtDecision, 2004);
# P and C are legacy codes seen only on a handful of 1991 motions.
MOTION_DEC_CODES <- c("G", "D", "M", "O", "W", "J", "P", "C")

lkp_motion_type <- read_eoir_lookup("inputs_eoir/tblLookupMotionType.csv")
stopifnot("PM" %in% lkp_motion_type$str_motion_code)

motions_raw <- read_eoir_tsv("inputs_eoir/tbl_Court_Motions.csv")

# Keep only pretermit motions before any cleaning so the rest of the script
# works on a small table. REC_TYPE is upper case in the extract; compare
# case-insensitively anyway so a stray lower-case code is not dropped.
pretermit <-
  motions_raw |>
  as_tibble() |>
  select(IDNMOTION, IDNCASE, REC_TYPE, DEC, COMP_DATE, MOTION_RECD_DATE) |>
  filter(str_to_upper(str_trim(REC_TYPE)) == "PM") |>
  clean_eoir_cols() |>
  mutate(DEC = str_to_upper(DEC))

n_pretermit_raw <- nrow(pretermit)
rm(motions_raw)
gc()

# Validate the raw pretermit rows before converting types
pretermit |>
  col_vals_not_null(IDNMOTION) |>
  col_vals_not_null(IDNCASE) |>
  col_vals_regex(IDNMOTION, "^\\d+$") |>
  col_vals_regex(IDNCASE, "^\\d+$") |>
  col_vals_in_set(DEC, c(MOTION_DEC_CODES, NA)) |>
  col_vals_regex(
    MOTION_RECD_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    COMP_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

pretermit <-
  pretermit |>
  fast_convert(list(
    IDNMOTION = "integer",
    IDNCASE = "integer",
    COMP_DATE = "date",
    MOTION_RECD_DATE = "date"
  )) |>
  transmute(
    idnmotion = IDNMOTION,
    idncase = IDNCASE,
    received_date = MOTION_RECD_DATE,
    completion_date = COMP_DATE,
    decision_code = DEC
  )

# A motion cannot be decided before it is received
pretermit |>
  col_vals_expr(
    expr(
      is.na(received_date) |
        is.na(completion_date) |
        received_date <= completion_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# Most recent = latest received date. Ties are broken by latest completion
# date and then highest motion id. desc() sorts NA last, so a motion with no
# received date is chosen only when it is the case's only pretermit motion.
pretermit_by_case <-
  pretermit |>
  arrange(idncase, desc(received_date), desc(completion_date), desc(idnmotion)) |>
  slice_head(n = 1, by = idncase) |>
  transmute(
    idncase,
    pretermit_motion_received_date_last = received_date,
    pretermit_motion_completion_date_last = completion_date,
    pretermit_motion_decision_code_last = decision_code
  )

# --- Checks that the selection is right ------------------------------------

# Independent recomputation of the per-case maximum received date
expected <-
  pretermit |>
  summarise(
    n_motions = n(),
    max_received = suppressWarnings(max(received_date, na.rm = TRUE)),
    .by = idncase
  ) |>
  mutate(max_received = if_else(is.infinite(max_received), NA, max_received))

check <-
  pretermit_by_case |>
  inner_join(expected, by = "idncase")

stopifnot(
  "no pretermit rows were dropped or duplicated" =
    sum(expected$n_motions) == n_pretermit_raw,
  "exactly one row per case" =
    nrow(pretermit_by_case) == n_distinct(pretermit$idncase),
  "every case with a pretermit motion is present" =
    nrow(check) == nrow(expected),
  "selected row has the latest received date" =
    all(
      check$pretermit_motion_received_date_last == check$max_received |
        (is.na(check$pretermit_motion_received_date_last) &
          is.na(check$max_received))
    )
)

# Selected rows are real rows from the input (date/decision come from the
# same motion record, not mixed across motions)
stopifnot(
  "selected rows match input rows" =
    nrow(semi_join(
      pretermit,
      pretermit_by_case |>
        rename(
          received_date = pretermit_motion_received_date_last,
          completion_date = pretermit_motion_completion_date_last,
          decision_code = pretermit_motion_decision_code_last
        ),
      by = c("idncase", "received_date", "completion_date", "decision_code")
    )) >= nrow(pretermit_by_case)
)

pretermit_by_case |>
  rows_distinct(idncase) |>
  col_vals_in_set(
    pretermit_motion_decision_code_last,
    c(MOTION_DEC_CODES, NA)
  ) |>
  invisible()

n_ties <- sum(
  (pretermit |> count(idncase, received_date) |> pull(n)) > 1
)
message(sprintf(
  "%d pretermit motions across %d cases; %d case-days with more than one pretermit motion (tie-broken by completion date, then motion id)",
  n_pretermit_raw,
  nrow(pretermit_by_case),
  n_ties
))
print(table(pretermit_by_case$pretermit_motion_decision_code_last, useNA = "ifany"))

arrow::write_parquet(
  pretermit_by_case,
  "tmp/motions_pretermit_cases.parquet"
)
