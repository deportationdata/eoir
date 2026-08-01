library(tidyverse)
library(duckplyr)
library(pointblank)

source("scripts/utilities.R")

# Fix mid-row tab shifts
custody_shift_finder <- make_shift_finder(
  date_cols = c("DATDETAINED", "DATRELEASED"),
  non_date_cols = c("CUSTODY")
)

custody_raw <- read_eoir_tsv("inputs_eoir/tbl_CustodyHistory.csv")

custody_fix_result <- auto_fix_tab_shifts(custody_raw, custody_shift_finder)

custodyhistory_by_case <-
  custody_fix_result$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Load lookup tables for validation
lkp_custody <- read_eoir_lookup("inputs_eoir/tblLookupCustodyStatus.csv")

# Validate columns before transforms
custodyhistory_by_case |>
  col_vals_not_null(
    IDNCUSTODY,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    CUSTODY,
    c(lkp_custody$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

custodyhistory_by_case <- fast_convert(
  custodyhistory_by_case,
  list(
    IDNCUSTODY = "integer",
    IDNCASE = "integer",
    DATDETAINED = "datetime",
    DATRELEASED = "datetime"
  )
)

custodyhistory_by_case <-
  custodyhistory_by_case |>
  janitor::clean_names() |>
  rename(
    date_detained = datdetained,
    date_released = datreleased
  ) |>
  arrange(idncase, date_detained, idncustody)

# Validate date ordering (detained should precede release)
custodyhistory_by_case |>
  col_vals_expr(
    expr(is.na(date_detained) | is.na(date_released) | date_detained <= date_released),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

# Row order within each idncase group is established by the arrange() above
# (date_detained, idncustody); nth() below relies on that order. nth() returns
# NA past the end of the group, matching the pad-to-length-4 behavior of the
# data.table block this replaces. Use `.by =` rather than group_by(): duckplyr
# cannot execute group_by() and silently falls back to plain dplyr, losing the
# speedup. arrange() afterwards because `.by =` returns groups in hash order.
custodyhistory_by_case <-
  custodyhistory_by_case |>
  summarise(
    detention_start_1 = nth(date_detained, 1),
    detention_start_2 = nth(date_detained, 2),
    detention_start_3 = nth(date_detained, 3),
    detention_start_4 = nth(date_detained, 4),
    detention_end_1 = nth(date_released, 1),
    detention_end_2 = nth(date_released, 2),
    detention_end_3 = nth(date_released, 3),
    detention_end_4 = nth(date_released, 4),
    .by = idncase
  ) |>
  arrange(idncase)

custodyhistory_by_case |>
  as_tibble() |>
  rows_distinct(idncase) |>
  # Detention periods should be in chronological order
  col_vals_expr(
    expr(is.na(detention_start_1) | is.na(detention_start_2) |
      detention_start_1 <= detention_start_2),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_expr(
    expr(is.na(detention_start_2) | is.na(detention_start_3) |
      detention_start_2 <= detention_start_3),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

arrow::write_parquet(
  custodyhistory_by_case,
  "tmp/custodyhistory_cases.parquet",
  compression = "ZSTD"
)
