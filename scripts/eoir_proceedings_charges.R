library(tidyverse)
library(tidylog)

charges_col_types <- c(
  IDNPRCDCHG = "integer",
  IDNCASE = "integer",
  IDNPROCEEDING = "integer",
  CHARGE = "character",
  CHG_STATUS = "character"
)

charges_tbl <-
  data.table::fread(
    "inputs_eoir/B_TblProceedCharges.csv",
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = c("", "NA", "N/A", "NULL"),
    colClasses = charges_col_types,
    fill = 6,
    showProgress = FALSE
  ) |>
  as_tibble()

proceedingscharges_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^B_TblProceedCharges\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(abs(nrow(charges_tbl) - proceedingscharges_count) < 5)

charges_tbl <-
  charges_tbl |>
  mutate(across(where(is.character), str_squish)) |>
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
