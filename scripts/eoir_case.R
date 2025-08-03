library(tidyverse)
library(tidylog)
library(data.table)

cases_tbl <- data.table::fread(
  "inputs/A_TblCase.csv",
  sep          = "\t",
  quote        = "",                   
  header       = TRUE,
  na.strings   = c("", "NA", "N/A", "NULL"),
  colClasses   = "character",         
  fill         = 47,                  
  showProgress = FALSE
)

cases_tbl <- 
  as_tibble(cases_tbl) |> 
  # remove extra columns created because of malformed rows
  select(-matches("^V\\d+$")) 

na_vals <- c("", "NA", "N/A", "NULL")

spec <- cols(
  IDNCASE           = col_integer(),
  LPR               = col_logical(),                    
  E_28_DATE         = col_datetime(format = ""),
  LATEST_HEARING    = col_datetime(format = ""),
  UP_BOND_DATE      = col_datetime(format = ""),
  DATE_OF_ENTRY     = col_datetime(format = ""),
  C_RELEASE_DATE    = col_datetime(format = ""),
  ADDRESS_CHANGEDON = col_datetime(format = ""),
  DATE_DETAINED     = col_datetime(format = ""),
  DATE_RELEASED     = col_datetime(format = ""),
  DETENTION_DATE    = col_datetime(format = ""),
  C_BIRTHDATE       = col_date(format = "")
)

# dt_cols <- names(spec$cols)[vapply(spec$cols, inherits, FUN.VALUE = logical(1),
#                                    what = "collector_datetime")]

# # codes in date columns that mean "pending / not yet set"
# clerk_codes <- c("M", "R", "B")

# # flag clerk codes in date columns and clean the date columns
# cases_tbl <- 
#   cases_tbl |>                     
#   mutate(
#     across(
#       all_of(dt_cols),
#       ~ if_else(. %in% clerk_codes, ., NA_character_),   
#       .names = "{.col}_clerk_flag"
#     ),
#     across(
#       all_of(dt_cols),
#       ~ if_else(. %in% clerk_codes, NA_character_, .)   
#     )
#   ) |> 
#   # drop all columns that end with _FLAG AND are all NA
#   select(!(ends_with("_FLAG") & where(~ all(is.na(.)))))

cases_tbl <-  type_convert(cases_tbl, col_types = spec, na = na_vals)

cases_tbl <- 
  cases_tbl |> 
  janitor::clean_names() |>
  select(
    -c(
      site_type, atty_nbr, update_site, latest_hearing, latest_time, latest_cal_type, up_bond_date, up_bond_rsn, correctional_fac, release_year, release_month, inmate_housing, updated_state, address_changedon, zbond_mrg_flag, dco_location, detention_facility_type
    )
  ) |> 
  mutate(
    e_28_date = as.Date(e_28_date)
  )

arrow::write_feather(
  cases_tbl,
  "outputs/cases_tmp.feather"
)


