library(tidyverse)
library(tidylog)

appeals_tbl <- data.table::fread("inputs_eoir/tblAppeal.csv")

appeals_count <-
  read_lines("inputs_eoir/Count.txt") |>
  keep(~ str_detect(., "^tblAppeal\\t")) |>
  str_extract("\\d+") |>
  as.integer()

stopifnot(abs(nrow(appeals_tbl) - appeals_count) < 5)

appeals_tbl <-
  appeals_tbl |>
  janitor::clean_names() |>
  mutate(
    datappealfiled = as.Date(dat_appeal_filed),
    datbiadec = as.Date(dat_bia_decision)
  ) |>
  arrange(idncase, datbiadec, datappealfiled, idn_appeal)

library(data.table)
setDT(appeals_tbl)

appeals_by_case <-
  appeals_tbl[,
    .(
      lastbiadecision = last(str_bia_decision),
      lastbiadecisiontype = last(str_bia_decision_type),
      lastappealcategory = last(str_appeal_category),
      lastappealtype = last(str_appeal_type),
      lastbiafiledby = last(str_filed_by),
      lastbiacustody = last(str_custody),
      last_e_27date = last(dat_attorney_e27),
      datbiadec = last(datbiadec),
      datappealfiled = last(datappealfiled),
      pendingappeal = case_when(
        any(is.na(datbiadec) & !is.na(datappealfiled)) ~ TRUE,
        TRUE ~ FALSE
      )
    ),
    by = .(idncase)
  ] |>
  as_tibble()

arrow::write_feather(
  appeals_by_case,
  "tmp/appeals_cases.feather"
)
