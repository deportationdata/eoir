library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

motions_raw <- read_eoir_tsv("inputs_eoir/tbl_Court_Motions.csv")

# The motions extract is ragged only in its tail columns (the *_Remove
# columns and strDJScenario carry extra tabs, and many rows stop early);
# every column used below is aligned for all row shapes, so the tab-shift
# repair used for other tables is not needed and would misfire on the
# tail overflow.
motions_tbl <-
  motions_raw |>
  as_tibble() |>
  select(
    IDNMOTION,
    IDNPROCEEDING,
    IDNCASE,
    REC_TYPE,
    DEC,
    COMP_DATE,
    MOTION_RECD_DATE,
    STRFILINGPARTY
  ) |>
  clean_eoir_cols() |>
  mutate(DEC = str_to_upper(DEC))

rm(motions_raw)
gc()

# Load lookup tables for validation
lkp_motion_type <- read_eoir_lookup("inputs_eoir/tblLookupMotionType.csv")
lkp_filing_party <- read_eoir_lookup(
  "inputs_eoir/tblLookupFiling_Method_Party.csv"
)

# The FOIA release has no lookup for motion decisions; these are the codes
# observed in the data. G/D/O/W follow the convention of other EOIR decision
# fields (grant, deny, other, withdrawn); M (which only appears from 2021 on,
# across routine motion types) is likely moot, and J/P/C are rare and
# unconfirmed, so decisions are kept as raw codes rather than labeled.
MOTION_DEC_CODES <- c("G", "D", "M", "O", "W", "J", "P", "C")

# Validate before transforms
motions_tbl |>
  col_vals_not_null(
    IDNMOTION,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNMOTION, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    REC_TYPE,
    c(lkp_motion_type$str_motion_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_in_set(
    DEC,
    c(MOTION_DEC_CODES, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    STRFILINGPARTY,
    c(lkp_filing_party$str_filing_party_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    COMP_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    MOTION_RECD_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

motions_tbl <- fast_convert(
  motions_tbl,
  list(
    IDNMOTION = "integer",
    IDNPROCEEDING = "integer",
    IDNCASE = "integer",
    COMP_DATE = "date",
    MOTION_RECD_DATE = "date"
  )
)

motions_tbl <-
  motions_tbl |>
  janitor::clean_names() |>
  rename(
    motion_type_code = rec_type,
    decision_code = dec,
    completion_date = comp_date,
    received_date = motion_recd_date,
    filing_party_code = strfilingparty
  )

# Case-level variables are built for substantively important motion types;
# scheduling, appearance, and counsel-change motions are not aggregated.
# Motions to reopen combine the general (O), in-absentia (MA), and
# changed-country-conditions (MC) reopening codes.
MOTION_FAMILIES <- c(
  O = "to_reopen",
  MA = "to_reopen",
  MC = "to_reopen",
  FD = "to_reconsider",
  C = "to_recalendar",
  CO = "for_continuance",
  CV = "for_change_of_venue",
  TP = "to_terminate",
  U = "to_dismiss",
  AC = "for_admin_closure",
  PM = "to_pretermit"
)

FAMILY_ORDER <- unique(MOTION_FAMILIES)

AGG_COLS <- c(
  "count",
  "filing_party_code_last",
  "received_date_last",
  "decision_code_last",
  "completion_date_last"
)

setDT(motions_tbl)

motions_tbl[, family := MOTION_FAMILIES[motion_type_code]]

family_motions <- motions_tbl[!is.na(family) & !is.na(idncase)]

# Validate date ordering (motion received before decided)
family_motions |>
  as_tibble() |>
  col_vals_expr(
    expr(
      is.na(received_date) |
        is.na(completion_date) |
        received_date <= completion_date
    ),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# Within a case and motion type, the "last" motion is the most recently
# received one, breaking ties by completion date and then record id; all
# _last columns for a type come from that same record, so a pending last
# motion shows its received date with an NA decision.
setorder(
  family_motions,
  idncase,
  family,
  received_date,
  completion_date,
  idnmotion,
  na.last = FALSE
)

motions_by_family <-
  family_motions[,
    .(
      count = .N,
      filing_party_code_last = last(filing_party_code),
      received_date_last = last(received_date),
      decision_code_last = last(decision_code),
      completion_date_last = last(completion_date)
    ),
    by = .(idncase, family)
  ]

motions_by_case <- dcast(
  motions_by_family,
  idncase ~ family,
  value.var = AGG_COLS
)

# dcast names columns <value>_<family>; rename to motion_<family>_<value>
# and group the columns by motion type
for (f in FAMILY_ORDER) {
  setnames(
    motions_by_case,
    paste(AGG_COLS, f, sep = "_"),
    paste("motion", f, AGG_COLS, sep = "_")
  )
}
setcolorder(
  motions_by_case,
  c(
    "idncase",
    unlist(lapply(FAMILY_ORDER, \(f) paste("motion", f, AGG_COLS, sep = "_")))
  )
)

# A case with any aggregated motion has a true count of zero for the other
# motion types (cases with no motions at all get zeros in the case join)
count_cols <- paste("motion", FAMILY_ORDER, "count", sep = "_")
motions_by_case[,
  (count_cols) := lapply(.SD, nafill, fill = 0L),
  .SDcols = count_cols
]

motions_by_case |>
  as_tibble() |>
  rows_distinct(idncase) |>
  col_vals_in_set(
    c(
      motion_to_reopen_decision_code_last,
      motion_to_reconsider_decision_code_last,
      motion_to_recalendar_decision_code_last,
      motion_for_continuance_decision_code_last,
      motion_for_change_of_venue_decision_code_last,
      motion_to_terminate_decision_code_last,
      motion_to_dismiss_decision_code_last,
      motion_for_admin_closure_decision_code_last,
      motion_to_pretermit_decision_code_last
    ),
    c(MOTION_DEC_CODES, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    c(
      motion_to_reopen_filing_party_code_last,
      motion_to_reconsider_filing_party_code_last,
      motion_to_recalendar_filing_party_code_last,
      motion_for_continuance_filing_party_code_last,
      motion_for_change_of_venue_filing_party_code_last,
      motion_to_terminate_filing_party_code_last,
      motion_to_dismiss_filing_party_code_last,
      motion_for_admin_closure_filing_party_code_last,
      motion_to_pretermit_filing_party_code_last
    ),
    c(lkp_filing_party$str_filing_party_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

arrow::write_parquet(
  motions_by_case,
  "tmp/motions_cases.parquet"
)
