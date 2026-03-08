library(tidyverse)
library(tidylog)

tblLanguage <- read_delim(
  "inputs_eoir/tblLanguage.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookup_CasePriority <- read_delim(
  "inputs_eoir/tblLookup_CasePriority.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookupAlienNat <- read_delim(
  "inputs_eoir/tblLookupAlienNat.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tbllookupAppealType <- read_delim(
  "inputs_eoir/tbllookupAppealType.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookupBaseCity <- read_delim(
  "inputs_eoir/tblLookupBaseCity.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tbllookupCharges <- read_delim(
  "inputs_eoir/tbllookupCharges.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookupBIA <- read_delim(
  "inputs_eoir/tblLookupBIA.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookupBIADecision <- read_delim(
  "inputs_eoir/tblLookupBIADecision.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookupHloc <- read_delim(
  "inputs_eoir/tblLookupHloc.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names() |>
  filter(hearing_loc_code != "IAD") # non-unique key with 88 different names for IAD; skip this lookup for now
tblLookupJudge <- read_delim(
  "inputs_eoir/tblLookupJudge.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()
tblLookupNationality <- read_delim(
  "inputs_eoir/tblLookupNationality.csv",
  delim = "\t",
  col_types = cols(.default = col_character()),
  na = c("", "NA", "NULL")
) |>
  janitor::clean_names()

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
    alien_zipcode = geoid_zcta5_20,
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
    zip_state = str_remove(ZIPName, " [(]PO boxes[)]$") |> str_extract("[A-Z]{2}$"),
    zip_city  = str_remove(ZIPName, " [(]PO boxes[)]$") |> str_remove(", [A-Z]{2}$")
  ) |>
  select(zcta, zip_city, zip_state)

zip_lookup <- left_join(zip_city, zcta_county, by = "alien_zipcode")
stopifnot(!anyDuplicated(zip_lookup$alien_zipcode))

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
    relief = if_else(outcome == "Relief Granted", TRUE, FALSE),
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
      replace_na,
      FALSE
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

# remove CSV read in errors based on a single variables
cases <-
  cases |> filter(is.na(c_asy_type) | c_asy_type %in% c("E", "I", "J"))

arrow::write_feather(
  cases,
  "outputs/cases.feather"
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
