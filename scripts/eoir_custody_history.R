library(tidyverse)
library(tidylog)
library(data.table)
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

custodyhistory_by_case <- type_convert(
  custodyhistory_by_case,
  col_types = cols(
    IDNCUSTODY = col_integer(),
    IDNCASE = col_integer(),
    DATDETAINED = col_date(),
    DATRELEASED = col_date()
  ),
  na = na_vals
)

check_parse(custodyhistory_by_case)

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

setDT(custodyhistory_by_case)

custodyhistory_by_case <-
  custodyhistory_by_case[,
    {
      det <- head(date_detained, 4L)
      rel <- head(date_released, 4L)
      # Pad to length 4 with NA
      length(det) <- 4L
      length(rel) <- 4L

      list(
        detention_start_1 = det[1L],
        detention_start_2 = det[2L],
        detention_start_3 = det[3L],
        detention_start_4 = det[4L],
        detention_end_1 = rel[1L],
        detention_end_2 = rel[2L],
        detention_end_3 = rel[3L],
        detention_end_4 = rel[4L]
      )
    },
    by = idncase
  ]

arrow::write_parquet(
  custodyhistory_by_case,
  "tmp/custodyhistory_cases.parquet"
)
