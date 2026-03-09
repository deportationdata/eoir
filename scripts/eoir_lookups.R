library(tidyverse)
library(tidylog)

court_decision_lookup <-
  data.table::fread("inputs_eoir/tblLookupCourtDecision.csv") |>
  as_tibble() |>
  janitor::clean_names()

dec_code_lookup <-
  court_decision_lookup |>
  filter(str_dec_type == "C") |>
  transmute(
    case_type = str_case_type,
    dec_code = str_dec_code,
    outcome = str_dec_description
  )

other_comp_code_lookup <-
  court_decision_lookup |>
  filter(str_dec_type == "O") |>
  transmute(
    case_type = str_case_type,
    other_comp = str_dec_code,
    other_completion = str_dec_description
  )

arrow::write_feather(dec_code_lookup, "tmp/dec_code_lookup.feather")
arrow::write_feather(
  other_comp_code_lookup,
  "tmp/other_comp_code_lookup.feather"
)
