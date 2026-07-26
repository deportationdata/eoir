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
tblLookupHloc <- read_eoir_lookup("inputs_eoir/tblLookupHloc.csv")
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
    # combine to find the last completion date whether IJ or BIA
    ij_or_bia_completion_date_last = if_else(
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
  rename(
    bond_hearing_location_code_first = hearing_location_code_first,
    bond_hearing_location_code_second = hearing_location_code_second,
    bond_hearing_location_code_last = hearing_location_code_last
  ) |>
  # now that the bond columns are prefixed, move the case-level hearing
  # location codes to the same first/last suffix style
  rename(
    hearing_location_code_first = first_hearing_location_code,
    hearing_location_code_last = last_hearing_location_code
  ) |>
  mutate(
    # Zero new_bond_amount is meaningful only when the IJ actually granted a bond;
    # for all other decisions the zero is an artifact, so replace with NA
    new_bond_amount_first = if_else(
      new_bond_amount_first == 0 &
        !bond_decision_first %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE",
            "FLORES - RELEASE"
          ),
      NA_real_,
      new_bond_amount_first
    ),
    new_bond_amount_second = if_else(
      new_bond_amount_second == 0 &
        !bond_decision_second %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE",
            "FLORES - RELEASE"
          ),
      NA_real_,
      new_bond_amount_second
    ),
    new_bond_amount_last = if_else(
      new_bond_amount_last == 0 &
        !bond_decision_last %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE",
            "FLORES - RELEASE"
          ),
      NA_real_,
      new_bond_amount_last
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
        asylum_decision_last,
        withholding_decision_last,
        cat_decision_last,
        adjustment_decision_last,
        non_lpr_cancellation_decision_last,
        lpr_cancellation_decision_last
      ),
      \(x) replace_na(x, "No application")
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
      asylum_claim_type_code,
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
    base_city_desc |> rename(bond_court_first = court_desc),
    by = c("bond_court_code_first" = "base_city_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    base_city_desc |> rename(bond_court_second = court_desc),
    by = c("bond_court_code_second" = "base_city_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    base_city_desc |> rename(bond_court_last = court_desc),
    by = c("bond_court_code_last" = "base_city_code"),
    relationship = "many-to-one"
  )

# Judge name
cases <- cases |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(first_judge_code = judge_code, first_judge_name = judge_name),
    by = "first_judge_code",
    relationship = "many-to-one"
  ) |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(last_judge_code = judge_code, last_judge_name = judge_name),
    by = "last_judge_code",
    relationship = "many-to-one"
  ) |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(judge_code, bond_judge_name_first = judge_name),
    by = c("bond_judge_code_first" = "judge_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(judge_code, bond_judge_name_second = judge_name),
    by = c("bond_judge_code_second" = "judge_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    tblLookupJudge |>
      filter(!is.na(judge_code)) |>
      select(judge_code, bond_judge_name_last = judge_name),
    by = c("bond_judge_code_last" = "judge_code"),
    relationship = "many-to-one"
  )

# Case and bond hearing locations
# (codes shared by multiple locations, e.g. IAD, cannot be resolved to a name)
hloc_desc <-
  tblLookupHloc |>
  filter(!is.na(hearing_loc_code)) |>
  add_count(hearing_loc_code) |>
  filter(n == 1) |>
  select(hearing_loc_code, hloc_name = hearing_loc_name)

cases <- cases |>
  left_join(
    hloc_desc |> rename(hearing_location_first = hloc_name),
    by = c("hearing_location_code_first" = "hearing_loc_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    hloc_desc |> rename(hearing_location_last = hloc_name),
    by = c("hearing_location_code_last" = "hearing_loc_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    hloc_desc |> rename(bond_hearing_location_first = hloc_name),
    by = c("bond_hearing_location_code_first" = "hearing_loc_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    hloc_desc |> rename(bond_hearing_location_second = hloc_name),
    by = c("bond_hearing_location_code_second" = "hearing_loc_code"),
    relationship = "many-to-one"
  ) |>
  left_join(
    hloc_desc |> rename(bond_hearing_location_last = hloc_name),
    by = c("bond_hearing_location_code_last" = "hearing_loc_code"),
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
    c(
      initial_bond_amount_first,
      initial_bond_amount_second,
      initial_bond_amount_last
    ),
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gte(
    c(new_bond_amount_first, new_bond_amount_second, new_bond_amount_last),
    0,
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # Zero new_bond_amount should only remain for grant decisions
  col_vals_gt(
    new_bond_amount_first,
    0,
    na_pass = TRUE,
    preconditions = \(x) {
      dplyr::filter(
        x,
        !bond_decision_first %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE"
          )
      )
    },
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gt(
    new_bond_amount_second,
    0,
    na_pass = TRUE,
    preconditions = \(x) {
      dplyr::filter(
        x,
        !bond_decision_second %in%
          c(
            "AMELIORATION GRANTED",
            "BOND GRANTED-AMOUNT DECREASED",
            "BOND GRANTED-OWN RECOGNIZANCE"
          )
      )
    },
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_gt(
    new_bond_amount_last,
    0,
    na_pass = TRUE,
    preconditions = \(x) {
      dplyr::filter(
        x,
        !bond_decision_last %in%
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
    last_judge_name,
    preconditions = \(x) dplyr::filter(x, !is.na(last_judge_code)),
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  col_vals_not_null(
    first_judge_name,
    preconditions = \(x) dplyr::filter(x, !is.na(first_judge_code)),
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
        !contains("state") &
        # county/place names arrive pre-cased from geocorr/Census;
        # str_to_title would mangle DeKalb, McLean, O'Brien, "Baltimore city"
        !any_of(c("county", "place")),
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
            "VD",
            "WCAT"
          )
        )
    ),
    across(
      c(
        first_court,
        final_court,
        bond_court_first,
        bond_court_second,
        bond_court_last
      ),
      # replace to title case but keep court codes in parentheses uppercase
      ~ str_replace(.x, "^([^(]+)", \(m) str_to_title(m))
    )
  ) |>
  rename(
    court_code_first = first_court_code,
    court_first = first_court,
    court_code_last = final_court_code,
    court_last = final_court,
    judge_code_first = first_judge_code,
    judge_first = first_judge_name,
    judge_code_last = last_judge_code,
    judge_last = last_judge_name,
    bond_judge_first = bond_judge_name_first,
    bond_judge_second = bond_judge_name_second,
    bond_judge_last = bond_judge_name_last,
    ij_completion_date_last = final_completion_date
  ) |>
  # Case length variables, in days
  mutate(
    nta_date_to_ij_completion_date_last_days = as.numeric(
      difftime(ij_completion_date_last, nta_date, units = "days")
    ),
    nta_date_to_ij_or_bia_completion_date_last_days = as.numeric(
      difftime(ij_or_bia_completion_date_last, nta_date, units = "days")
    ),
    nta_date_to_bond_completion_date_first_days = as.numeric(
      difftime(bond_completion_date_first, nta_date, units = "days")
    ),
    nta_date_to_bond_completion_date_second_days = as.numeric(
      difftime(bond_completion_date_second, nta_date, units = "days")
    ),
    nta_date_to_bond_completion_date_last_days = as.numeric(
      difftime(bond_completion_date_last, nta_date, units = "days")
    ),
    appeal_filed_date_to_bia_decision_date_days = as.numeric(
      difftime(bia_decision_date, appeal_filed_date, units = "days")
    ),
    bond_hearing_request_date_first_to_bond_completion_date_first_days = as.numeric(
      difftime(
        bond_completion_date_first,
        bond_hearing_request_date_first,
        units = "days"
      )
    ),
    bond_hearing_request_date_second_to_bond_completion_date_second_days = as.numeric(
      difftime(
        bond_completion_date_second,
        bond_hearing_request_date_second,
        units = "days"
      )
    ),
    bond_hearing_request_date_last_to_bond_completion_date_last_days = as.numeric(
      difftime(
        bond_completion_date_last,
        bond_hearing_request_date_last,
        units = "days"
      )
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
    entry_date,
    nta_date,
    charge_section_1,
    charge_section_2,
    charge_section_3,
    charge_section_4,

    # Court & judge
    court_code_first,
    court_first,
    court_code_last,
    court_last,
    hearing_location_code_first,
    hearing_location_first,
    hearing_location_code_last,
    hearing_location_last,
    judge_code_first,
    judge_first,
    judge_code_last,
    judge_last,

    # IJ proceedings
    deported_1_code,
    deported_1,
    deported_2_code,
    deported_2,
    e28_date,
    represented,
    in_absentia,
    ij_completion_date_last,
    ij_or_bia_completion_date_last,

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
    bond_court_code_first,
    bond_court_first,
    bond_hearing_location_code_first,
    bond_hearing_location_first,
    bond_judge_code_first,
    bond_judge_first,
    bond_hearing_request_date_first,
    bond_completion_date_first,
    bond_decision_first,
    initial_bond_amount_first,
    new_bond_amount_first,

    bond_court_code_second,
    bond_court_second,
    bond_hearing_location_code_second,
    bond_hearing_location_second,
    bond_judge_code_second,
    bond_judge_second,
    bond_hearing_request_date_second,
    bond_completion_date_second,
    bond_decision_second,
    initial_bond_amount_second,
    new_bond_amount_second,

    bond_court_code_last,
    bond_court_last,
    bond_hearing_location_code_last,
    bond_hearing_location_last,
    bond_judge_code_last,
    bond_judge_last,
    bond_hearing_request_date_last,
    bond_completion_date_last,
    bond_decision_last,
    initial_bond_amount_last,
    new_bond_amount_last,

    # Applications for relief
    asylum_claim_type_code,
    asylum_claim_type,
    asylum_decision_last,
    withholding_decision_last,
    cat_decision_last,
    adjustment_decision_last,
    non_lpr_cancellation_decision_last,
    lpr_cancellation_decision_last,

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

    # Case lengths in days
    nta_date_to_ij_completion_date_last_days,
    nta_date_to_ij_or_bia_completion_date_last_days,
    nta_date_to_bond_completion_date_first_days,
    nta_date_to_bond_completion_date_second_days,
    nta_date_to_bond_completion_date_last_days,
    appeal_filed_date_to_bia_decision_date_days,
    bond_hearing_request_date_first_to_bond_completion_date_first_days,
    bond_hearing_request_date_second_to_bond_completion_date_second_days,
    bond_hearing_request_date_last_to_bond_completion_date_last_days
  )

# filter cases for final dataset
cases <-
  cases |>
  filter(
    # keep data after IIRIRA which changed many codes in the data
    !is.na(nta_date) & nta_date >= as.Date("1997-10-01"),
    # keep
    # * 1. standard removal proceedings (all new such cases will be RMV after 1996)
    # * 2. withholding-only proceedings (WHO)
    case_type_code %in% c("RMV", "WHO")
  ) |>
  select(-case_type_code, -case_type)

arrow::write_parquet(
  cases,
  "data/cases.parquet",
  compression = "ZSTD"
)
