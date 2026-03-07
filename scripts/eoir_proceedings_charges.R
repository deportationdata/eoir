library(tidyverse)
library(tidylog)

charges_tbl <- data.table::fread(
  "inputs/B_TblProceedCharges.csv",
  fill = 6
) |>
  as_tibble()

proceedingscharges_count <-
  read_lines("inputs/Count.txt") |>
  keep(~ str_detect(., "^B_TblProceedCharges\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(abs(nrow(charges_tbl) - proceedingscharges_count) < 5)

charges_tbl <-
  charges_tbl |>
  janitor::clean_names()

library(data.table)

charges_dt <- as.data.table(charges_tbl)

setorder(charges_dt, idncase, idnproceeding, idnprcdchg)

charges_by_case <- charges_dt[,
  .(
    charge_1 = charge[1L],
    charge_2 = charge[2L],
    charge_3 = charge[3L],
    charge_4 = charge[4L],
    charges_all = paste(charge, collapse = "; ")
  ),
  by = idncase
]

arrow::write_feather(
  charges_by_case |> as_tibble(),
  "tmp/charges_cases.feather"
)
