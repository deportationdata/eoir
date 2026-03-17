library(tidyverse)
library(tidylog)

source("scripts/utilities.R")

court_decision_lookup <-
  read_eoir_lookup("inputs_eoir/tblLookupCourtDecision.csv")

dec_code_lookup <-
  court_decision_lookup |>
  filter(str_dec_type == "C") |>
  transmute(
    case_type_code = str_case_type,
    dec_code = str_dec_code,
    case_outcome = str_dec_description
  )

other_comp_code_lookup <-
  court_decision_lookup |>
  filter(str_dec_type == "O") |>
  transmute(
    case_type_code = str_case_type,
    other_comp = str_dec_code,
    other_completion = str_dec_description
  )

arrow::write_parquet(dec_code_lookup, "tmp/dec_code_lookup.parquet")
arrow::write_parquet(
  other_comp_code_lookup,
  "tmp/other_comp_code_lookup.parquet"
)
