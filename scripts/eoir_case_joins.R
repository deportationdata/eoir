library(tidyverse)
library(tidylog)

cases <- 
  arrow::read_feather("outputs/cases_from_proceedings.feather")

custodyhistory_by_case <- 
  arrow::read_feather("outputs/custodyhistory_cases.feather")

cases <- 
  cases |> 
  left_join(custodyhistory_by_case, by = "idncase") 

rm(custodyhistory_by_case)
gc()

case_tbl <- arrow::read_feather("outputs/cases_tmp.feather")

case_tbl <- 
  case_tbl |> 
  select(
    idncase,
    !any_of(colnames(cases))
  )

cases <- 
  cases |> 
  inner_join(case_tbl, by = "idncase")

rm(case_tbl)
gc()

appeals_by_case <- 
  arrow::read_feather("outputs/appeals_cases.feather")

cases <-
  cases |> 
  left_join(appeals_by_case, by = "idncase")

rm(appeals_by_case)
gc()

# replace finalcompdate=datbiadec if datbiadec > finalcompd & datbiadec < .
# convert stata to r
cases <-
  cases |> 
  mutate(
    finalcompdate = if_else(
      !is.na(datbiadec) & datbiadec > finalcompdate,
      datbiadec,
      finalcompdate
    )
  )

court_applications_by_case <- 
  arrow::read_feather("outputs/court_applications_cases.feather") 

cases <-
  cases |> 
  left_join(court_applications_by_case, by = "idncase")

rm(court_applications_by_case)
gc()

associated_bond_by_case <- 
  arrow::read_feather("outputs/associated_bond_cases.feather")

cases <-
  cases |> 
  left_join(associated_bond_by_case, by = "idncase")

rm(associated_bond_by_case)
gc()

charges_by_case <- arrow::read_feather("outputs/charges_cases.feather")

cases <- 
  cases |> 
  left_join(charges_by_case, by = "idncase")

other_comp_code_lookup <- 
  arrow::read_feather("outputs/other_comp_code_lookup.feather")

dec_code_lookup <- 
  arrow::read_feather("outputs/dec_code_lookup.feather")

cases <- 
  cases |> 
  left_join(dec_code_lookup, by = c("case_type", "dec_code")) |> 
  left_join(other_comp_code_lookup, by = c("case_type", "other_comp"))

cases <-
  cases |> 
  mutate(
    outcome = case_when(outcome == "" ~ other_completion, TRUE ~ outcome),
    relief = if_else(outcome == "Relief Granted", TRUE, FALSE),
    termination = if_else(outcome %in% c("Terminate", "Terminated"), TRUE, FALSE),
    finalcompyear = year(finalcompdate),
    length = as.numeric(finalcompdate - osc_date),
    anyreliefapp = case_when(anyreliefapp != 1 ~ 0, TRUE ~ anyreliefapp)
  )

arrow::write_feather(
  cases,
  "outputs/cases.feather"
)



arrow::write_feather(cases, "outputs/cases.feather")
haven::write_dta(cases, "outputs/cases.dta")
haven::write_sav(cases, "outputs/cases.sav")

# split into sheets for Excel (max 1 million rows per sheet)
sheet_size <- 1e6
n_sheets <- ceiling(nrow(cases) / sheet_size)

cases |>
  mutate(.sheet_id = rep(seq_len(n_sheets), each = sheet_size, length.out = n())) |>
    #  gl(n_sheets, sheet_size, n(), labels = FALSE)) |>
  group_split(.sheet_id, .keep = FALSE) |>
  set_names(sprintf("%s_%02d", "Sheet", seq_len(n_sheets))) |>
  writexl::write_xlsx("outputs/cases.xlsx")
