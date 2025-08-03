library(tidyverse)
library(tidylog)

court_decision_lookup <- 
  data.table::fread("inputs/Lookup/tblLookupCourtDecision.csv") |> 
  as_tibble()

dec_code_lookup <- 
  court_decision_lookup |> 
  janitor::clean_names() |> 
  filter(str_dec_type == "C") |> 
  transmute(
      case_type = str_case_type,
      dec_code = str_dec_code,
      outcome = str_dec_description
  ) 
    
arrow::write_feather(
  dec_code_lookup,
  "outputs/dec_code_lookup.feather"
)

rm(list=ls())
gc()

court_decision_lookup <- 
  data.table::fread("inputs/Lookup/tblLookupCourtDecision.csv") |> 
  as_tibble()

other_comp_code_lookup <- 
  court_decision_lookup |> 
  janitor::clean_names() |> 
  filter(str_dec_type == "O") |> 
  transmute(
      case_type = str_case_type,
      other_comp = str_dec_code,
      other_completion = str_dec_description
  ) 
    
arrow::write_feather(
  other_comp_code_lookup,
  "outputs/other_comp_code_lookup.feather"
)