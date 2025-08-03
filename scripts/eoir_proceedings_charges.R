library(tidyverse)
library(tidylog)

charges_tbl <- data.table::fread("inputs/B_TblProceedCharges.csv", fill = 6) |> as_tibble()

source("scripts/categories_df.R")

charges_tbl <- 
  charges_tbl |> 
  janitor::clean_names()

charges_tbl <-
  charges_tbl |> 
  inner_join(categories, by = "charge")

charges_tbl <- 
  charges_tbl |> 
  mutate(
    crim = if_else(B == "Criminal", 1, 0),
    agg = if_else(B == "Agg_Felon", 1, 0),
    cimt = if_else(B == "CIMT", 1, 0),
    ctrlsub = if_else(B == "Ctrl_Sub", 1, 0),
    ewi = if_else(B == "EWI", 1, 0),
    ulfp = if_else(B == "Unlf_pre", 1, 0),
    reentry = if_else(B == "Re-entry", 1, 0),
    fraud = if_else(B == "Doc_Fraud", 1, 0),
    subsdeport = if_else(B == "237a02Bi", 1, 0),
    other = if_else(crim == 0 & agg == 0 & cimt == 0 & ctrlsub == 0 & ewi == 0 & ulfp == 0 & reentry == 0 & fraud == 0, 1, 0)
  )

library(data.table)
setDT(charges_tbl)

charges_by_case <- 
  charges_tbl[, .(
    crim = sum(crim),
    agg = sum(agg),
    cimt = sum(cimt),
    ctrlsub = sum(ctrlsub),
    ewi = sum(ewi),
    ulfp = sum(ulfp),
    reentry = sum(reentry),
    fraud = sum(fraud),
    subsdeport = sum(subsdeport),
    other = sum(other)
  ), by = idncase] |> 
  as_tibble()

arrow::write_feather(
  charges_by_case,
  "outputs/charges_cases.feather"
)
