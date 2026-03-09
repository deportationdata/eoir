library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/utilities.R")

charges_col_types <- c(
  IDNPRCDCHG = "integer",
  IDNCASE = "integer",
  IDNPROCEEDING = "integer",
  CHARGE = "character",
  CHG_STATUS = "character"
)

charges_tbl <-
  read_eoir_tsv(
    "inputs_eoir/B_TblProceedCharges.csv",
    col_types = charges_col_types
  ) |>
  as_tibble() |>
  clean_eoir_cols() |>
  janitor::clean_names()

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
