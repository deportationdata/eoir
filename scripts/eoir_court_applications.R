library(tidyverse)
library(tidylog)

court_applications_tbl <- data.table::fread("inputs/tbl_Court_Appln.csv", fill = 7)

court_applications_tbl <- 
  court_applications_tbl |> 
  janitor::clean_names()

library(data.table)
setDT(court_applications_tbl)
court_applications_by_case <- 
  court_applications_tbl[, .(
    asylumapp = any(appl_code == "ASYL"),
    withholdapp = any(appl_code == "ASYW"),
    catapp = any(appl_code == "WCAT"),
    adjustapp = any(appl_code == "245"),
    nonlprcancelapp = any(appl_code == "42B"),
    lprcancelapp = any(appl_code == "42A"),
    anyreliefapp = any(!is.na(appl_code) & appl_code != "VD")
  ), by = idncase] |> 
  as_tibble() |> 
  filter(!is.na(idncase))

arrow::write_feather(
  court_applications_by_case,
  "outputs/court_applications_cases.feather"
)
