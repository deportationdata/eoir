library(tidyverse)
library(tidylog)

source("scripts/eoir_utils.R")

custodyhistory_col_types <- c(
  IDNCUSTODY = "integer",
  IDNCASE = "integer",
  CUSTODY = "character",
  DATDETAINED = "POSIXct",
  DATRELEASED = "POSIXct"
)

custodyhistory_tbl <-
  data.table::fread(
    "inputs_eoir/tbl_CustodyHistory.csv",
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = c("", "NA", "N/A", "NULL"),
    colClasses = custodyhistory_col_types,
    fill = 5,
    showProgress = FALSE
  ) |>
  as_tibble()

custodyhistory_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^tbl_CustodyHistory\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(nrow(custodyhistory_tbl) == custodyhistory_count)

custodyhistory_by_case <-
  custodyhistory_tbl |>
  as_tibble() |>
  clean_string_cols() |>
  janitor::clean_names() |>
  # convert to date to remove unused time information
  mutate(
    datdetained = as.Date(datdetained), # no changes to missing
    datreleased = as.Date(datreleased) # no changes to missing
  ) |>
  arrange(idncase, datdetained, idncustody)

rm(custodyhistory_tbl)
gc()

# rewrite in data.table
library(data.table)
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
