
library(tidyverse)
library(tidylog)

custodyhistory_tbl <- data.table::fread("inputs/tbl_CustodyHistory.csv") |> as_tibble()

custodyhistory_by_case <- 
  custodyhistory_tbl |> 
  janitor::clean_names() |> 
  # convert to date to remove unused time information
  mutate(
    datdetained = as.Date(datdetained), # no changes to missing
    datreleased = as.Date(datreleased)  # no changes to missing
  ) |> 
  arrange(idncase, datdetained)

rm(custodyhistory_tbl)
gc()

# rewrite in data.table 
library(data.table)
setDT(custodyhistory_by_case)
custodyhistory_by_case <- 
  custodyhistory_by_case[
    , {
      det_max <- max(datdetained, na.rm = TRUE)
      rel_max <- max(datreleased, na.rm = TRUE)
      first_det <- min(datdetained, na.rm = TRUE)

      # Convert rel_max = -Inf to NA
      rel_max <- if (is.finite(rel_max)) rel_max else as.Date(NA)

      last_release <- if (is.finite(det_max) && any(datreleased < det_max, na.rm = TRUE)) {
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
  "outputs/custodyhistory_cases.feather"
)

