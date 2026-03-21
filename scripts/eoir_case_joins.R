library(tidyverse)
library(tidylog)
library(pointblank)

source("scripts/utilities.R")

tblLanguage <- read_eoir_lookup("inputs_eoir/tblLanguage.csv")
tblLookup_CasePriority <- read_eoir_lookup(
  "inputs_eoir/tblLookup_CasePriority.csv"
)
tblLookupAlienNat <- read_eoir_lookup("inputs_eoir/tblLookupAlienNat.csv")
tbllookupAppealType <- read_eoir_lookup("inputs_eoir/tbllookupAppealType.csv")
tblLookupBaseCity <- read_eoir_lookup("inputs_eoir/tblLookupBaseCity.csv")
tblLookupBIADecision <- read_eoir_lookup("inputs_eoir/tblLookupBIADecision.csv")
tblLookupJudge <- read_eoir_lookup("inputs_eoir/tblLookupJudge.csv")
tblLookupBIADecisionType <- read_eoir_lookup(
  "inputs_eoir/tblLookupBIADecisionType.csv"
)
tblLookupFiledBy <- read_eoir_lookup("inputs_eoir/tblLookupFiledBy.csv")
tblLookupCaseType <- read_eoir_lookup("inputs_eoir/tblLookupCaseType.csv")
tblLookupSex <- read_eoir_lookup("inputs_eoir/tblLookupSex.csv")

cases <-
  arrow::read_parquet("tmp/cases_from_proceedings.parquet")

custodyhistory_by_case <-
  arrow::read_parquet("tmp/custodyhistory_cases.parquet")

cases <-
  cases |>
  left_join(custodyhistory_by_case, by = "idncase")

rm(custodyhistory_by_case)
gc()

case_tbl <- arrow::read_parquet("tmp/cases_tmp.parquet")

case_tbl <-
  case_tbl |>
  select(
    idncase,
    !any_of(colnames(cases))
  )

n_before_case <- nrow(cases)

cases <-
  cases |>
  inner_join(case_tbl, by = "idncase")

message(sprintf(
  "inner_join with case table: %d -> %d rows (%d dropped)",
  n_before_case,
  nrow(cases),
  n_before_case - nrow(cases)
))

rm(case_tbl)
gc()

zip_lookup <- arrow::read_parquet("tmp/zip_lookup.parquet")

n_before_zip <- nrow(cases)

cases <- cases |>
  left_join(
    zip_lookup,
    by = c("alien_zipcode" = "zcta"),
    relationship = "many-to-one"
  ) |>
  select(-alien_zipcode)

cases |>
  row_count_match(n_before_zip) |>
  # Zip merge should not introduce too many NAs
  # col_vals_not_null(
  #   state,
  #   actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  # ) |>
  # col_vals_not_null(
  #   county,
  #   actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  # ) |>
  col_vals_in_set(
    state,
    c(state.abb, "DC", "AS", "GU", "MP", "PR", "VI", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_regex(
    county_fips,
    "^\\d{5}$",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

appeals_by_case <-
  arrow::read_parquet("tmp/appeals_cases.parquet")

cases <-
  cases |>
  left_join(appeals_by_case, by = "idncase")

rm(appeals_by_case)
gc()

# replace final_completion_date with bia_decision_date if BIA decided later
n_bia_override <- sum(
  !is.na(cases$bia_decision_date) &
    (is.na(cases$final_completion_date) |
      cases$bia_decision_date > cases$final_completion_date),
  na.rm = TRUE
)
message(sprintf(
  "final_completion_date overridden by bia_decision_date for %d cases",
  n_bia_override
))


cases <-
  cases |>
  mutate(
    # keep the last IJ decision date
    ij_final_date = final_completion_date,
    # combine to find the last completion date whether IJ or BIA
    final_completion_date = if_else(
      !is.na(bia_decision_date) &
        !is.na(final_completion_date) &
        bia_decision_date > final_completion_date,
      bia_decision_date,
      final_completion_date
    )
  )

court_applications_by_case <-
  arrow::read_parquet("tmp/court_applications_cases.parquet")

cases <-
  cases |>
  left_join(court_applications_by_case, by = "idncase")

rm(court_applications_by_case)
gc()

associated_bond_by_case <-
  arrow::read_parquet("tmp/associated_bond_cases.parquet")

cases <-
  cases |>
  left_join(associated_bond_by_case, by = "idncase")

rm(associated_bond_by_case)
gc()

charges_by_case <- arrow::read_parquet("tmp/charges_cases.parquet")

cases <-
  cases |>
  left_join(charges_by_case, by = "idncase")

rm(charges_by_case)
gc()

other_comp_code_lookup <-
  arrow::read_parquet("tmp/other_comp_code_lookup.parquet")

dec_code_lookup <-
  arrow::read_parquet("tmp/dec_code_lookup.parquet")

cases <-
  cases |>
  left_join(
    dec_code_lookup,
    by = c("case_type_code", "dec_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    other_comp_code_lookup,
    by = c("case_type_code", "other_comp"),
    relationship = "many-to-one"
  )

cases <-
  cases |>
  mutate(
    case_outcome = coalesce(case_outcome, other_completion),
    relief_granted = case_outcome %in% "Relief Granted",
    terminated = case_outcome %in% c("Terminate", "Terminated"),
    final_completion_year = year(final_completion_date),
    case_length_days = as.numeric(
      difftime(final_completion_date, nta_date, units = "days")
    ),
    across(
      c(
        asylum_application,
        withholding_application,
        cat_application,
        adjustment_application,
        non_lpr_cancellation_application,
        lpr_cancellation_application,
        any_relief_application
      ),
      \(x) replace_na(x, FALSE)
    )
  ) |>
  select(-dec_code, -other_comp, -other_completion)

# Recode custody and asylum claim type codes to human-readable labels
cases <-
  cases |>
  mutate(
    custody_code = recode_values(
      custody,
      "N" ~ "never detained",
      "R" ~ "released",
      "D" ~ "detained throughout"
    ),
    asylum_claim_type = recode_values(
      asylum_claim_type,
      "I" ~ "affirmative",
      "E" ~ "defensive",
      "J" ~ "J"
    ),
    custody_at_appeal_code = recode_values(
      custody_at_appeal_code,
      "N" ~ "never detained",
      "R" ~ "released",
      "D" ~ "detained throughout"
    )
  ) |>
  select(-custody)

# Resolve code columns to human-readable descriptions via lookup tables

# Language
cases <- cases |>
  rename(language_code = lang) |>
  left_join(
    tblLanguage |>
      filter(!is.na(str_code)) |>
      select(str_code, language = str_description),
    by = c("language_code" = "str_code"),
    relationship = "many-to-one"
  )

# Case priority
cases <- cases |>
  left_join(
    tblLookup_CasePriority |>
      filter(!is.na(str_code)) |>
      select(str_code, case_priority = str_description),
    by = c("case_priority_code" = "str_code"),
    relationship = "many-to-one"
  )

# Nationality
cases <- cases |>
  rename(nationality_code = nat) |>
  left_join(
    tblLookupAlienNat |>
      filter(!is.na(str_code)) |> # TODO: discuss with David - Netherlands Antilles was NA
      select(str_code, nationality = str_description),
    by = c("nationality_code" = "str_code"),
    relationship = "many-to-one"
  )

# Appeal type
cases <- cases |>
  rename(appeal_type_code = appeal_type) |>
  left_join(
    tbllookupAppealType |>
      filter(!is.na(str_appl_code)) |>
      select(str_appl_code, appeal_type = str_appl_description),
    by = c("appeal_type_code" = "str_appl_code"),
    relationship = "many-to-one"
  )

# BIA decision
cases <- cases |>
  rename(bia_decision_code = bia_decision) |>
  left_join(
    tblLookupBIADecision |>
      filter(!is.na(str_code)) |>
      select(str_code, bia_decision = str_description),
    by = c("bia_decision_code" = "str_code"),
    relationship = "many-to-one"
  )

# Courts (first + final)
base_city_desc <-
  tblLookupBaseCity |>
  filter(!is.na(base_city_code)) |>
  transmute(
    base_city_code,
    court_desc = glue::glue("{base_city} ({base_city_code})")
  )

cases <- cases |>
  rename(first_court_code = first_court, final_court_code = final_court) |>
  left_join(
    base_city_desc |> rename(first_court = court_desc),
    by = c("first_court_code" = "base_city_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    base_city_desc |> rename(final_court = court_desc),
    by = c("final_court_code" = "base_city_code"),
    relationship = "many-to-one"
  )

# Judge name
cases <- cases |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(judge_code, judge_name),
    by = "judge_code",
    relationship = "many-to-one"
  )

# BIA decision type
cases <- cases |>
  left_join(
    tblLookupBIADecisionType |>
      filter(!is.na(str_code)) |>
      select(str_code, bia_decision_type = str_description),
    by = c("bia_decision_type_code" = "str_code"),
    relationship = "many-to-one"
  )

# Appeal filed by
cases <- cases |>
  left_join(
    tblLookupFiledBy |>
      filter(!is.na(str_code)) |>
      select(str_code, appeal_filed_by = str_description),
    by = c("appeal_filed_by_code" = "str_code"),
    relationship = "many-to-one"
  )

# Case type
cases <- cases |>
  left_join(
    tblLookupCaseType |>
      filter(!is.na(str_code)) |>
      select(str_code, case_type = str_description),
    by = c("case_type_code" = "str_code"),
    relationship = "many-to-one"
  )

# Sex
cases <- cases |>
  left_join(
    tblLookupSex |>
      filter(!is.na(strcode)) |>
      select(strcode, sex = str_description),
    by = c("sex_code" = "strcode"),
    relationship = "many-to-one"
  )

# Validate final assembled dataset
cases |>
  col_vals_not_null(
    idncase,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    asylum_claim_type,
    c("affirmative", "defensive", "J", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    case_type_code,
    c(tblLookupCaseType$str_code, "BND", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    bia_decision_type_code,
    c(tblLookupBIADecisionType$str_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    custody_code,
    c("never detained", "released", "detained throughout", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # Check that new lookup joins resolved most values
  col_vals_not_null(
    bia_decision_type,
    preconditions = \(x) dplyr::filter(x, !is.na(bia_decision_type_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    appeal_filed_by,
    preconditions = \(x) dplyr::filter(x, !is.na(appeal_filed_by_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    case_type,
    preconditions = \(x) dplyr::filter(x, !is.na(case_type_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_gte(
    case_length_days,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_between(
    final_completion_year,
    1985L,
    as.integer(format(Sys.Date(), "%Y")) + 1L,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  invisible()

cases <- cases |>
  mutate(across(where(is.POSIXct), ~ as.Date(.x, tz = "UTC"))) |>
  filter(
    # keep data after IIRIRA which changed many codes in the data
    !is.na(nta_date) & nta_date >= as.Date("1997-10-01")
  ) |>
  relocate(
    # Case identifiers
    idncase,

    # Case type information
    case_type_code,
    case_type,
    case_priority_code,
    case_priority,

    # Respondent demographics
    sex_code,
    sex,
    birth_year,
    nationality_code,
    nationality,
    language_code,
    language,

    # Geography
    state,
    state_fips,
    county,
    county_fips,
    place,
    place_fips,

    # Entry & initiation
    date_of_entry,
    nta_date,
    charge_section_1,
    charge_section_2,
    charge_section_3,
    charge_section_4,

    # Court & judge
    first_court_code,
    first_court,
    final_court_code,
    final_court,
    first_hearing_location_code,
    last_hearing_location_code,
    judge_code,
    judge_name,

    # IJ proceedings
    first_proceeding_date,
    e28_date,
    in_absentia,
    ij_final_date,
    final_completion_date,
    final_completion_year,
    case_length_days,

    # Custody & detention
    custody_code,
    detention_start_1,
    detention_end_1,
    detention_start_2,
    detention_end_2,
    detention_start_3,
    detention_end_3,
    detention_start_4,
    detention_end_4,

    # Bond
    bond_hearing_request_date,
    bond_completion_date,
    bond_decision,
    initial_bond_amount,
    new_bond_amount,

    # Applications for relief
    asylum_claim_type,
    asylum_application,
    withholding_application,
    cat_application,
    adjustment_application,
    non_lpr_cancellation_application,
    lpr_cancellation_application,
    any_relief_application,

    # IJ outcome
    case_outcome,
    relief_granted,
    terminated,

    # BIA appeal
    appeal_type_code,
    appeal_type,
    appeal_filed_by_code,
    appeal_filed_by,
    appeal_filed_date,
    e27_date,
    custody_at_appeal_code,
    bia_decision_code,
    bia_decision,
    bia_decision_type_code,
    bia_decision_type,
    bia_decision_date
  )

arrow::write_parquet(
  cases,
  "outputs/cases.parquet",
  compression = "ZSTD"
)

arrow::write_parquet(
  cases |>
    select(
      -ends_with("_code"),
      first_hearing_location_code,
      last_hearing_location_code,
      custody_code,
      custody_at_appeal_code
    ),
  "outputs/cases-no-codes.parquet",
  compression = "ZSTD"
)

# haven::write_dta(cases, "outputs/cases.dta")
