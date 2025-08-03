library(tidyverse)
library(tidylog)

appeals_tbl <- data.table::fread("inputs/tblAppeal.csv")

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
  appeals_tbl[, .(
    lastbiadecision = last(str_bia_decision),
    lastbiadecisiontype = last(str_bia_decision_type),
    lastappealcategory = last(str_appeal_category), 
    lastappealtype = last(str_appeal_type),
    lastbiafiledby = last(str_filed_by),
    lastbiacustody = last(str_custody),
    last_e_27date = last(dat_attorney_e27),
    datbiadec = last(datbiadec),
    datappealfiled = last(datappealfiled),
    pendingappeal = any(is.na(datbiadec) & !is.na(datappealfiled))
  ), by = .(idncase)] |> 
  as_tibble()

arrow::write_feather(
  appeals_by_case,
  "outputs/appeals_cases.feather"
)

