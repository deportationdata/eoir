library(tidyverse)
library(tidylog)
library(data.table)

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
  clean_eoir_cols() |>
  janitor::clean_names() |>
  # convert to date to remove unused time information
  mutate(
    datdetained = as.Date(datdetained), # no changes to missing
    datreleased = as.Date(datreleased) # no changes to missing
  ) |>
  arrange(idncase, datdetained, idncustody)

setDT(custodyhistory_by_case)

custodyhistory_by_case <-
  custodyhistory_by_case[,
    {
      det_max <- max(datdetained, na.rm = TRUE)
      rel_max <- max(datreleased, na.rm = TRUE)
      first_det <- min(datdetained, na.rm = TRUE)

      # Convert rel_max = -Inf to NA
      rel_max <- if (is.finite(rel_max)) rel_max else as.Date(NA)

      last_release <- if (
        is.finite(det_max) && any(datreleased < det_max, na.rm = TRUE)
      ) {
        NA_Date_
      } else {
        rel_max
      }

      list(
        firstdetained = if (is.finite(first_det)) first_det else as.Date(NA),
        lastreleased = last_release,
        lastcustody = tail(na.omit(custody), 1L)
      )
    },
    by = idncase
  ] |>
  as_tibble()

arrow::write_feather(
  custodyhistory_by_case,
  "tmp/custodyhistory_cases.feather"
)
