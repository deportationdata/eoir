library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

custodyhistory_col_types <- c(
  IDNCUSTODY = "integer",
  IDNCASE = "integer",
  CUSTODY = "character",
  DATDETAINED = "POSIXct",
  DATRELEASED = "POSIXct"
)

custodyhistory_by_case <-
  read_eoir_tsv(
    "inputs_eoir/tbl_CustodyHistory.csv",
    col_types = custodyhistory_col_types
  ) |>
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
  )

custodyhistory_by_case <-
  custodyhistory_by_case |>
  janitor::clean_names() |>
  # convert to date to remove unused time information
  mutate(
    datdetained = as.Date(datdetained), # no changes to missing
    datreleased = as.Date(datreleased) # no changes to missing
  ) |>
  arrange(idncase, datdetained, idncustody)

# Validate date ordering (detained should precede release)
custodyhistory_by_case |>
  col_vals_expr(
    expr(is.na(datdetained) | is.na(datreleased) | datdetained <= datreleased),
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  )

setDT(custodyhistory_by_case)

custodyhistory_by_case <-
  custodyhistory_by_case[,
    {
      det <- head(datdetained, 4L)
      rel <- head(datreleased, 4L)
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
