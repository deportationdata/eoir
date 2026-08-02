library(tidyverse)
library(duckplyr)
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

charges_tbl <- charges_tbl |>
  arrange(idncase, idnproceeding, idnprcdchg) |>
  # Freeze the sort order into a column: DuckDB does not guarantee a GROUP BY
  # feeds rows to an aggregate in input order, so nth() below is told the
  # order explicitly via order_by = row_order. See scripts/eoir_proceeding.R
  # for the full note.
  mutate(row_order = row_number())

# Row order within each idncase group is established by the arrange() above
# and passed to each aggregate via order_by = row_order. nth() returns NA past
# the end of the group (matching out-of-bounds `[1L]` etc. indexing in the
# data.table block this replaces). Use `.by =` rather than group_by():
# duckplyr cannot execute group_by() and silently falls back to plain dplyr,
# losing the speedup. arrange() afterwards because `.by =` returns groups in
# hash order.
charges_by_case <- charges_tbl |>
  summarise(
    charge_section_1 = nth(charge_str, 1, order_by = row_order),
    charge_section_2 = nth(charge_str, 2, order_by = row_order),
    charge_section_3 = nth(charge_str, 3, order_by = row_order),
    charge_section_4 = nth(charge_str, 4, order_by = row_order),
    .by = idncase
  ) |>
  arrange(idncase)

charges_by_case |>
  as_tibble() |>
  rows_distinct(idncase) |>
  # Every case with charges should have at least charge_section_1
  col_vals_not_null(
    charge_section_1,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

arrow::write_parquet(
  charges_by_case,
  "tmp/charges_cases.parquet",
  compression = "ZSTD"
)
