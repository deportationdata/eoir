library(tidyverse)
library(tidylog)

setwd("~/Library/CloudStorage/Box-Box/deportationdata/")

associated_bond_tbl <- data.table::fread("eoir/courts/070125/D_TblAssociatedBond.csv", fill = 33)

associated_bond_tbl <- 
  associated_bond_tbl |> 
  janitor::clean_names() |> 
  mutate(
    bond_comp_date = as.Date(comp_date),
    bond_hear_req_date = as.Date(bond_hear_req_date)
  ) |> 
  arrange(idncase, bond_comp_date, idnassocbond)

library(data.table)
setDT(associated_bond_tbl)

associated_bond_by_case <- 
  associated_bond_tbl[, .(
    lastbond_comp_date = last(comp_date),
    lastbond_hear_req_date = last(bond_hear_req_date),
    lastdec = last(dec),
    lastinitial_bond = last(initial_bond),
    lastnew_bond = last(new_bond)
  ), by = idncase] |> 
  as_tibble()

arrow::write_feather(
  associated_bond_by_case,
  "~/github/deportation-cleaning/tmp/associated_bond_cases.feather"
)
