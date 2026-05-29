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

# Inner join should not drop more than 1% of cases
if (nrow(cases) < n_before_case * 0.99) {
  warning(sprintf(
    "inner_join with case table dropped %.1f%% of rows",
    (1 - nrow(cases) / n_before_case) * 100
  ))
}

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
    county_fips_code,
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
  left_join(associated_bond_by_case, by = "idncase") |>
  mutate(
    # Zero new_bond_amount is meaningful only when the IJ actually granted a bond;
    # for all other decisions the zero is an artifact, so replace with NA
    new_bond_amount = if_else(
      new_bond_amount == 0 &
        !bond_decision %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE",
            "FLORES - RELEASE"
          ),
      NA_real_,
      new_bond_amount
    )
  )

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
    # relief_granted = case_outcome %in% "Relief Granted",
    # terminated = case_outcome %in% c("Terminate", "Terminated"),
    # final_completion_year = year(final_completion_date),
    # case_length_days = as.numeric(
    #   difftime(final_completion_date, nta_date, units = "days")
    # ),
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
    custody = recode_values(
      custody_code,
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
    custody_at_appeal = recode_values(
      custody_at_appeal_code,
      "N" ~ "never detained",
      "R" ~ "released",
      "D" ~ "detained throughout"
    )
  )

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
      filter(!is.na(str_code)) |>
      select(str_code, nationality = str_description),
    by = c("nationality_code" = "str_code"),
    relationship = "many-to-one"
  )

# Deported-to country (same lookup as nationality)
cases <- cases |>
  left_join(
    tblLookupAlienNat |>
      filter(!is.na(str_code)) |>
      select(str_code, deported_1 = str_description),
    by = c("deported_1_code" = "str_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    tblLookupAlienNat |>
      filter(!is.na(str_code)) |>
      select(str_code, deported_2 = str_description),
    by = c("deported_2_code" = "str_code"),
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
  left_join(
    base_city_desc |> rename(first_court = court_desc),
    by = c("first_court_code" = "base_city_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    base_city_desc |> rename(final_court = court_desc),
    by = c("final_court_code" = "base_city_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    base_city_desc |> rename(bond_court = court_desc),
    by = c("bond_court_code" = "base_city_code"),
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
  ) |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(judge_code, bond_judge_name = judge_name),
    by = c("bond_judge_code" = "judge_code"),
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
    custody,
    c("never detained", "released", "detained throughout", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # Bond amounts should be non-negative when present
  col_vals_gte(
    initial_bond_amount,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    new_bond_amount,
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # Zero new_bond_amount should only remain for grant decisions
  col_vals_gt(
    new_bond_amount,
    0,
    na_pass = TRUE,
    preconditions = \(x) {
      dplyr::filter(
        x,
        !bond_decision %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE"
          )
      )
    },
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # Check that lookup joins resolved most values
  col_vals_not_null(
    language,
    preconditions = \(x) dplyr::filter(x, !is.na(language_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    nationality,
    preconditions = \(x) dplyr::filter(x, !is.na(nationality_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    deported_1,
    preconditions = \(x) dplyr::filter(x, !is.na(deported_1_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    deported_2,
    preconditions = \(x) dplyr::filter(x, !is.na(deported_2_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    sex,
    preconditions = \(x) dplyr::filter(x, !is.na(sex_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    case_priority,
    preconditions = \(x) dplyr::filter(x, !is.na(case_priority_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    case_type,
    preconditions = \(x) dplyr::filter(x, !is.na(case_type_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    judge_name,
    preconditions = \(x) dplyr::filter(x, !is.na(judge_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    appeal_type,
    preconditions = \(x) dplyr::filter(x, !is.na(appeal_type_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    appeal_filed_by,
    preconditions = \(x) dplyr::filter(x, !is.na(appeal_filed_by_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    bia_decision,
    preconditions = \(x) dplyr::filter(x, !is.na(bia_decision_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    bia_decision_type,
    preconditions = \(x) dplyr::filter(x, !is.na(bia_decision_type_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  # Check that recodes resolved values (custody/custody_at_appeal should not be NA when code is present)
  col_vals_not_null(
    custody,
    preconditions = \(x) dplyr::filter(x, !is.na(custody_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    custody_at_appeal,
    preconditions = \(x) dplyr::filter(x, !is.na(custody_at_appeal_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

make_abbr_caps <- function(x, abbr) {
  for (a in abbr) {
    pattern <- if (str_detect(a, "/")) a else paste0("\\b", a, "\\b")
    x <- str_replace_all(x, regex(pattern, ignore_case = TRUE), a)
  }
  x
}

cases <-
  cases |>
  mutate(
    across(where(is.POSIXct), ~ as.Date(.x, tz = "UTC")),
    across(
      where(is.character) &
        !contains("_code") &
        !contains("_court") &
        !contains("judge_name") &
        !contains("charge_section") &
        !contains("fips") &
        !contains("state"),
      ~ str_to_title(.x) |>
        make_abbr_caps(
          abbr = c(
            "ABC",
            "BIA",
            "CAT",
            "CFV",
            "CPC",
            "DHS",
            "EOIR",
            "IJ",
            "INA",
            "ORR",
            "PD",
            "ROP",
            "V/D",
            "VD"
          )
        )
    ),
    across(
      c(first_court, final_court),
      # replace to title case but keep court codes in parentheses uppercase
      ~ str_replace(.x, "^([^(]+)", \(m) str_to_title(m))
    )
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
    state_fips_code,
    county,
    county_fips_code,
    place,
    place_fips_code,

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
    e28_date,
    in_absentia,
    ij_final_date,
    final_completion_date,

    # Custody & detention
    custody_code,
    custody,
    detention_start_1,
    detention_end_1,
    detention_start_2,
    detention_end_2,
    detention_start_3,
    detention_end_3,
    detention_start_4,
    detention_end_4,

    # Bond
    bond_court_code,
    bond_court,
    bond_judge_code,
    bond_judge_name,
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

    # BIA appeal
    appeal_type_code,
    appeal_type,
    appeal_filed_by_code,
    appeal_filed_by,
    appeal_filed_date,
    e27_date,
    custody_at_appeal_code,
    custody_at_appeal,
    bia_decision_code,
    bia_decision,
    bia_decision_type_code,
    bia_decision_type,
    bia_decision_date,

    # Removal
    deported_1_code,
    deported_1,
    deported_2_code,
    deported_2
  )

# filter cases for final dataset
cases <-
  cases |>
  filter(
    # keep data after IIRIRA which changed many codes in the data
    !is.na(nta_date) & nta_date >= as.Date("1997-10-01"),
    # keep only standard removal proceedings (all new cases will be RMV after 1996)
    case_type_code == "RMV"
  ) |>
  select(-case_type_code, -case_type)

arrow::write_parquet(
  cases,
  "data/cases.parquet",
  compression = "ZSTD"
)

arrow::write_parquet(
  cases |>
    select(
      !ends_with("_code") |
        any_of(c(
          "first_hearing_location_code",
          "last_hearing_location_code"
        ))
    ),
  "data/cases-no-codes.parquet",
  compression = "ZSTD"
)
