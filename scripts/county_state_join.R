library(tidyverse)
library(tidylog)
library(pointblank)

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
  filter(!is.na(geoid_zcta5_20)) |>
  group_by(geoid_zcta5_20) |>
  slice_max(arealand_part, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(
    zcta = geoid_zcta5_20,
    respondent_county = namelsad_county_20,
    respondent_county_fips = geoid_county_20
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
    respondent_state = str_remove(ZIPName, " [(]PO boxes[)]$") |>
      str_extract("[A-Z]{2}$"),
    respondent_city = str_remove(ZIPName, " [(]PO boxes[)]$") |>
      str_remove(", [A-Z]{2}$")
  ) |>
  select(zcta, respondent_city, respondent_state) |>
  filter(!is.na(respondent_city), !is.na(respondent_state))

# --- Validation checks ---

us_50_plus_dc <- c(state.abb, "DC")
territories <- c("AS", "GU", "MP", "PR", "VI")

# Validate zcta_county
zcta_county |>
  col_vals_not_null(zcta) |>
  col_vals_not_null(respondent_county) |>
  col_vals_not_null(respondent_county_fips) |>
  col_vals_regex(zcta, "^\\d{5}$") |>
  col_vals_regex(respondent_county_fips, "^\\d{5}$") |>
  rows_distinct(zcta) |>
  # 2020 Census has ~33,120 ZCTAs
  col_vals_between(
    n_zcta,
    left = 30000,
    right = 35000,
    preconditions = \(x) tibble(n_zcta = n_distinct(x$zcta))
  ) |>
  # Census has 3,143 counties in 50 states + DC
  col_vals_between(
    n_counties,
    left = 3100,
    right = 3500,
    preconditions = \(x) {
      tibble(n_counties = n_distinct(x$respondent_county_fips))
    }
  ) |>
  invisible()

# Validate zip_city
zip_city |>
  col_vals_not_null(zcta) |>
  col_vals_not_null(respondent_state) |>
  col_vals_not_null(respondent_city) |>
  col_vals_regex(zcta, "^\\d{5}$") |>
  rows_distinct(zcta) |>
  # All state codes are valid US states or territories
  col_vals_in_set(
    respondent_state,
    c(us_50_plus_dc, territories),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  # All 50 states + DC are represented
  col_vals_make_set(
    respondent_state,
    set = us_50_plus_dc
  ) |>
  # 2020 Census geocorr should have 30,000–40,000 ZCTAs
  col_vals_between(
    n_zcta,
    left = 30000,
    right = 40000,
    preconditions = \(x) tibble(n_zcta = n_distinct(x$zcta))
  ) |>
  invisible()

# Combine into a single lookup
zip_lookup <- left_join(zip_city, zcta_county, by = "zcta")

# Validate joined lookup
zip_lookup |>
  rows_distinct(zcta) |>
  col_vals_not_null(respondent_state) |>
  col_vals_not_null(respondent_city) |>
  # County can be missing for ZCTAs not in Census county crosswalk
  col_vals_not_null(
    respondent_county,
    actions = action_levels(warn_at = 0.01, stop_at = 0.05)
  ) |>
  invisible()

arrow::write_parquet(zip_lookup, "outputs/zip_lookup.parquet")
