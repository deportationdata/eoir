
library(tidyverse)
library(tidylog)

proceeding_col_types <- c(
  IDNPROCEEDING               = "integer",
  IDNCASE                     = "integer",
  OSC_DATE                    = "POSIXct",
  INPUT_DATE                  = "POSIXct",
  BASE_CITY_CODE             = "character",
  HEARING_LOC_CODE           = "character",
  IJ_CODE                    = "character",
  TRANS_IN_DATE              = "POSIXct",
  PREV_HEARING_LOC           = "character",
  PREV_HEARING_BASE          = "character",
  PREV_IJ_CODE               = "character",
  TRANS_NBR                  = "character",
  HEARING_DATE               = "POSIXct",
  HEARING_TIME               = "character",
  DEC_TYPE                   = "character",
  DEC_CODE                   = "character",
  DEPORTED_1                 = "character",
  DEPORTED_2                 = "character",
  OTHER_COMP                 = "character",
  APPEAL_RSVD                = "character",
  APPEAL_NOT_FILED           = "character",
  COMP_DATE                  = "POSIXct",
  ABSENTIA                   = "character",
  VENUE_CHG_GRANTED          = "POSIXct",
  TRANSFER_TO                = "character",
  DATE_APPEAL_DUE_STATUS     = "POSIXct",
  TRANSFER_STATUS            = "character",
  CUSTODY                    = "character",
  CASE_TYPE                  = "character",
  NAT                        = "character",
  LANG                       = "character",
  SCHEDULED_HEAR_LOC         = "character",
  CORRECTIONAL_FAC           = "character",
  CRIM_IND                   = "character",
  IHP                        = "character",
  AGGRAVATE_FELON            = "logical",   # bit → logical
  DATE_DETAINED              = "POSIXct",
  DATE_RELEASED              = "POSIXct"
)

proceeding_tbl <- 
  data.table::fread("inputs/B_TblProceeding.csv",
    sep          = "\t",
    quote        = "",                   
    header       = TRUE,
    na.strings   = c("", "NA", "N/A", "NULL"),
    colClasses   = proceeding_col_types,         
    fill         = 40,                  
    showProgress = FALSE
  ) |> 
  as_tibble()

cases_from_proceedings <- 
  proceeding_tbl |> 
  janitor::clean_names() |> 
  # convert OSC_DATE and COMP_DATE to date to remove unused time information
  mutate(
    osc_date = as.Date(osc_date),
    comp_date = as.Date(comp_date) 
  ) |> 
  # clean up absentia column which has erroneous values due to csv errors
  # assumes missing values, date errors, and "X", "DEP", and "5" values are not absentia
  mutate(
    absentia = case_when(absentia == "Y" ~ TRUE, TRUE ~ FALSE)
  ) |> 
  # drop rows with missing IDNCASE (creating a case-level dataset)
  # -2 rows
  filter(!is.na(idncase)) |>
  # drop cases with inconsistent case types
  # -307 rows
  filter(n_distinct(case_type) == 1, .by = "idncase") |> 
  arrange(idncase, comp_date, dec_code, other_comp, nat, lang, idnproceeding)

rm(proceeding_tbl)
gc()

library(data.table)
setDT(cases_from_proceedings)

cases_from_proceedings <- 
  cases_from_proceedings[, .(
    firstcompdate = first(comp_date),
    finalcompdate = last(comp_date),
    osc_date = first(osc_date),
    firstcourt = first(base_city_code),
    finalcourt = last(base_city_code),
    case_type = first(case_type),
    dec_code = last(dec_code),
    other_comp = last(other_comp),
    absentia = last(absentia),
    nat = last(nat),
    lang = last(lang),
    custody = last(custody),
    firsthearingloc = first(hearing_loc_code),
    lasthearingloc = last(hearing_loc_code),
    ij_code = last(ij_code)
  ), by = idncase] |> 
  as_tibble()

cases_from_proceedings <- 
  cases_from_proceedings |>
  as_tibble()

arrow::write_feather(
  cases_from_proceedings,
  "outputs/cases_from_proceedings.feather"
)

