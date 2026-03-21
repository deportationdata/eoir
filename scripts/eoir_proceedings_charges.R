library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

# Fix mid-row tab shifts
charges_shift_finder <- make_shift_finder(
  date_cols = character(0),
  non_date_cols = c("CHARGE", "CHG_STATUS")
)

charges_raw <- read_eoir_tsv("inputs_eoir/B_TblProceedCharges.csv")

charges_fix_result <- auto_fix_tab_shifts(charges_raw, charges_shift_finder)

charges_tbl <-
  charges_fix_result$dt |>
  as_tibble() |>
  clean_eoir_cols()

# Validate before transforms
charges_tbl |>
  col_vals_not_null(
    IDNPRCDCHG,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNPROCEEDING,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNPRCDCHG, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_not_null(
    CHARGE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_regex(
    CHARGE,
    "^(212|237|241|242|246|215)[a-zA-Z]\\d",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.001, stop_at = 0.01)
  ) |>
  col_vals_in_set(
    CHG_STATUS,
    c("N", "O", "S", "W", "s", "w", NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

charges_tbl <- fast_convert(
  charges_tbl,
  list(
    IDNPRCDCHG = "integer",
    IDNCASE = "integer",
    IDNPROCEEDING = "integer"
  )
)

charges_tbl <-
  charges_tbl |>
  janitor::clean_names()

# Parse charge codes into INA section citation format
# e.g. "212a6Ci" -> "212(a)(6)(C)(i)", "237a2Biv" -> "237(a)(2)(B)(iv)"
charges_tbl <- charges_tbl |>
  mutate(
    # first remove any punctuation and white space from the original charge string to get a clean code to parse
    charge_str = str_remove_all(charge, "[[:punct:]\\s]+"),
    # Extract the numeric INA section prefix (e.g. "212", "237")
    section = str_extract(charge_str, "^\\d+"),
    # Parse the subsection portion into parenthesized citation format
    remainder = str_remove(charge_str, "^\\d+") |>
      # Lowercase the first letter when followed by a digit (e.g. "A6" -> "a6")
      # since the first subsection letter is always lowercase in INA citations
      str_replace("^[A-Z](?=[0-9])", "a") |>
      # Insert ")(" at each case or type boundary to split into subsection parts
      str_replace_all(
        "(?<=[a-z])(?=[A-Z0-9])|(?<=[A-Z])(?=[a-z0-9])|(?<=[0-9])(?=[A-Za-z])|\\s+",
        ")("
      ) |>
      # Wrap the whole remainder in parentheses
      str_replace("^(.+)$", "(\\1)") |>
      # Strip leading zeros from numeric subsections (e.g. "(01)" -> "(1)")
      str_replace_all("\\(0+(\\d)", "(\\1") |>
      # Remove any empty parentheses produced by edge cases
      str_replace_all("\\(\\)", ""),

    # Combine section and parsed remainder into final citation string
    charge_str = if_else(
      is.na(section),
      NA_character_,
      glue::glue("{section}{remainder}")
    )
  )

charges_tbl <-
  charges_tbl |>
  mutate(
    charge_str = case_when(
      charge == "212a03F" ~ "212(a)(3)(F)",
      charge == "215a" ~ "215(a)", # not used but in case it appears in future data
      charge == "215b" ~ "215(b)", # not used but in case it appears in future data
      charge == "237s02AiI" ~ "237(a)(2)(A)(i)(I)",
      charge == "2153g" ~ "215(g)",
      charge == "2153h" ~ "215(h)",
      TRUE ~ charge_str
    )
  )

# Deduplicate charges within each proceeding
charges_tbl <- charges_tbl |>
  distinct(idncase, idnproceeding, charge_str, .keep_all = TRUE)

setDT(charges_tbl)

setorder(charges_tbl, idncase, idnproceeding, idnprcdchg)

charges_by_case <- charges_tbl[,
  .(
    charge_section_1 = charge_str[1L],
    charge_section_2 = charge_str[2L],
    charge_section_3 = charge_str[3L],
    charge_section_4 = charge_str[4L]
  ),
  by = idncase
]

arrow::write_parquet(
  charges_by_case,
  "tmp/charges_cases.parquet"
)
