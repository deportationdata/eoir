library(tidyverse)
library(tidylog)

source("scripts/utilities.R")

tblLanguage <- read_eoir_lookup("inputs_eoir/tblLanguage.csv")
tblLookup_CasePriority <- read_eoir_lookup(
  "inputs_eoir/tblLookup_CasePriority.csv"
)
tblLookupAlienNat <- read_eoir_lookup("inputs_eoir/tblLookupAlienNat.csv")
tbllookupAppealType <- read_eoir_lookup("inputs_eoir/tbllookupAppealType.csv")
tblLookupBaseCity <- read_eoir_lookup("inputs_eoir/tblLookupBaseCity.csv")
tbllookupCharges <- read_eoir_lookup("inputs_eoir/tbllookupCharges.csv")
tblLookupBIA <- read_eoir_lookup("inputs_eoir/tblLookupBIA.csv")
tblLookupBIADecision <- read_eoir_lookup("inputs_eoir/tblLookupBIADecision.csv")
tblLookupHloc <- read_eoir_lookup("inputs_eoir/tblLookupHloc.csv") |>
  filter(hearing_loc_code != "IAD") # non-unique key; skip for now
tblLookupJudge <- read_eoir_lookup("inputs_eoir/tblLookupJudge.csv")
tblLookupNationality <- read_eoir_lookup("inputs_eoir/tblLookupNationality.csv")

cases <-
  arrow::read_feather("tmp/cases_from_proceedings.feather")

custodyhistory_by_case <-
  arrow::read_feather("tmp/custodyhistory_cases.feather")

cases <-
  cases |>
  left_join(custodyhistory_by_case, by = "idncase")

rm(custodyhistory_by_case)
gc()

case_tbl <- arrow::read_feather("tmp/cases_tmp.feather")

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

# --- ZIP code → city + county (Census/USPS) ---
zcta_county_url <- "https://www2.census.gov/geo/docs/maps-data/data/rel2020/zcta520/tab20_zcta520_county20_natl.txt"
zcta_county_path <- "inputs/tab20_zcta520_county20_natl.txt"
if (!file.exists(zcta_county_path)) {
  download.file(zcta_county_url, zcta_county_path)
}
zcta_county <- read_delim(
  zcta_county_path,
  delim = "|",
  col_types = cols(.default = col_character(), AREALAND_PART = col_double())
) |>
  janitor::clean_names() |>
  group_by(geoid_zcta5_20) |>
  slice_max(arealand_part, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(
    zcta = geoid_zcta5_20,
    zip_county = namelsad_county_20
  )

# City name from Missouri Census Data Center geocorr file (USPS ZIPName field)
zip_city <- read_csv(
  "inputs/geocorr2022_2606509064.csv",
  skip = 2,
  col_names = c(
    "zcta",
    "state",
    "place",
    "stab",
    "PlaceName",
    "ZIPName",
    "pop20",
    "afact"
  ),
  col_types = cols(
    .default = col_character(),
    pop20 = col_double(),
    afact = col_double()
  )
) |>
  filter(!is.na(zcta)) |>
  distinct(zcta, ZIPName) |>
  mutate(
    zip_state = str_remove(ZIPName, " [(]PO boxes[)]$") |>
      str_extract("[A-Z]{2}$"),
    zip_city = str_remove(ZIPName, " [(]PO boxes[)]$") |>
      str_remove(", [A-Z]{2}$")
  ) |>
  select(zcta, zip_city, zip_state)

zip_lookup <- left_join(zip_city, zcta_county, by = "zcta")
stopifnot(!anyDuplicated(zip_lookup$zcta))

n_before_zip <- nrow(cases)
cases <- cases |>
  left_join(zip_lookup, by = c("alien_zipcode" = "zcta")) |>
  select(-alien_zipcode)
stopifnot(nrow(cases) == n_before_zip)

appeals_by_case <-
  arrow::read_feather("tmp/appeals_cases.feather")

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
    # keep the last IJ decision date
    last_ij_decision = finalcompdate,
    # combine to find the last completion date whether IJ or BIA
    finalcompdate = if_else(
      !is.na(datbiadec) & datbiadec > finalcompdate,
      datbiadec,
      finalcompdate
    )
  )

court_applications_by_case <-
  arrow::read_feather("tmp/court_applications_cases.feather")

cases <-
  cases |>
  left_join(court_applications_by_case, by = "idncase")

rm(court_applications_by_case)
gc()

associated_bond_by_case <-
  arrow::read_feather("tmp/associated_bond_cases.feather")

cases <-
  cases |>
  left_join(associated_bond_by_case, by = "idncase")

rm(associated_bond_by_case)
gc()

charges_by_case <- arrow::read_feather("tmp/charges_cases.feather")

cases <-
  cases |>
  left_join(charges_by_case, by = "idncase")

other_comp_code_lookup <-
  arrow::read_feather("tmp/other_comp_code_lookup.feather")

dec_code_lookup <-
  arrow::read_feather("tmp/dec_code_lookup.feather")

cases <-
  cases |>
  left_join(dec_code_lookup, by = c("case_type", "dec_code")) |>
  left_join(other_comp_code_lookup, by = c("case_type", "other_comp"))

cases <-
  cases |>
  mutate(
    outcome = coalesce(outcome, other_completion),
    relief = outcome %in% "Relief Granted",
    termination = if_else(
      outcome %in% c("Terminate", "Terminated"),
      TRUE,
      FALSE
    ),
    finalcompyear = year(finalcompdate),
    length = as.numeric(finalcompdate - osc_date),
    across(
      c(
        asylumapp,
        withholdapp,
        catapp,
        adjustapp,
        nonlprcancelapp,
        lprcancelapp,
        anyreliefapp
      ),
      \(x) replace_na(x, FALSE)
    )
  )

# tblLanguage: lang -> str_code
cases <- cases |>
  left_join(
    tblLanguage |>
      select(str_code, str_description) |>
      rename(lang_desc = str_description),
    by = c("lang" = "str_code")
  ) |>
  select(-lang)

# tblLookup_CasePriority: casepriority_code -> str_code
cases <- cases |>
  left_join(
    tblLookup_CasePriority |>
      select(str_code, str_description) |>
      rename(casepriority_desc = str_description),
    by = c("casepriority_code" = "str_code")
  ) |>
  select(-casepriority_code)

# tblLookupAlienNat: nat -> str_code (tblLookupNationality also matches but this is more specific)
cases <- cases |>
  left_join(
    tblLookupAlienNat |>
      select(str_code, str_description) |>
      rename(nat_desc = str_description),
    by = c("nat" = "str_code")
  ) |>
  select(-nat)

# tbllookupAppealType: lastappealtype -> str_appl_code
cases <- cases |>
  left_join(
    tbllookupAppealType |>
      select(str_appl_code, str_appl_description) |>
      rename(lastappealtype_desc = str_appl_description),
    by = c("lastappealtype" = "str_appl_code")
  ) |>
  select(-lastappealtype)

# tblLookupBaseCity: firstcourt and finalcourt -> base_city_code (two joins)
cases <-
  cases |>
  left_join(
    tblLookupBaseCity |>
      transmute(
        base_city_code,
        firstcourt_city_code = glue::glue("{base_city} ({base_city_code})")
      ),
    by = c("firstcourt" = "base_city_code")
  ) |>
  left_join(
    tblLookupBaseCity |>
      transmute(
        base_city_code,
        finalcourt_city_code = glue::glue("{base_city} ({base_city_code})")
      ),
    by = c("finalcourt" = "base_city_code")
  ) |>
  select(-firstcourt, -finalcourt)

# tblLookupBIADecision: lastbiadecision -> str_code
cases <- cases |>
  left_join(
    tblLookupBIADecision |>
      select(str_code, str_description) |>
      rename(lastbiadecision_desc = str_description),
    by = c("lastbiadecision" = "str_code")
  ) |>
  select(-lastbiadecision)

# tblLookupJudge: ij_code -> judge_code
cases <-
  cases |>
  left_join(
    tblLookupJudge |> select(judge_code, judge_name),
    by = c("ij_code" = "judge_code")
  ) |>
  select(-ij_code)

# --- Data validation: NA-ify values from confirmed CSV read-in errors ---
cases <-
  cases |>
  mutate(
    # NA-ify bad lastbiadecisiontype values (source data error, not a valid code)
    lastbiadecisiontype = if_else(
      lastbiadecisiontype %in% c("A", "L", "P", "R", "T"),
      lastbiadecisiontype,
      NA_character_
    )
  )

# Load additional lookup tables for final validation
lkp_case_type <- read_eoir_lookup("inputs_eoir/tblLookupCaseType.csv")
lkp_bia_dec_type <- read_eoir_lookup("inputs_eoir/tblLookupBIADecisionType.csv")
lkp_custody <- read_eoir_lookup("inputs_eoir/tblLookupCustodyStatus.csv")

# Validate final assembled dataset
cases |>
  col_vals_not_null(
    idncase,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    c_asy_type,
    c("E", "I", "J", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    case_type,
    c(lkp_case_type$str_code, "BND", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    lastbiadecisiontype,
    c(lkp_bia_dec_type$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    custody,
    c(lkp_custody$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    length,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    finalcompyear,
    1990L,
    as.integer(format(Sys.Date(), "%Y")) + 1L,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  )

# --- Flag rows with known source-data anomalies ---
cases <-
  cases |>
  mutate(
    data_flag = case_when(
      !is.na(alien_state) & !grepl("^[A-Z]{2}$", alien_state) ~
        "unusual_alien_state",
      !is.na(detention_location) & grepl("^[0-9]+$", detention_location) ~
        "unusual_detention_location",
      TRUE ~ NA_character_
    )
  )

arrow::write_feather(
  cases,
  "outputs/cases.feather"
)

arrow::write_parquet(
  cases,
  "outputs/cases.parquet",
  compression = "ZSTD"
)

# haven::write_dta(cases, "outputs/cases.dta")
# haven::write_sav(cases, "outputs/cases.sav")

# # split into sheets for Excel (max 1 million rows per sheet)
# sheet_size <- 1e6
# n_sheets <- ceiling(nrow(cases) / sheet_size)

# cases |>
#   mutate(
#     .sheet_id = rep(seq_len(n_sheets), each = sheet_size, length.out = n())
#   ) |>
#   #  gl(n_sheets, sheet_size, n(), labels = FALSE)) |>
#   group_split(.sheet_id, .keep = FALSE) |>
#   set_names(sprintf("%s_%02d", "Sheet", seq_len(n_sheets))) |>
#   writexl::write_xlsx("outputs/cases.xlsx")
