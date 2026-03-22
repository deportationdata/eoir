library(tidyverse)
library(tidylog)
library(pointblank)

# --- ZIP → county, state lookup for EOIR respondent geography ---
#
# MEASUREMENT NOTES:
#   - alien_zipcode in EOIR data is the address of record with the immigration
#     court — not necessarily a residential address. It may reflect an attorney's
#     office, a detention facility, a sponsor's address, or a stale address from
#     initial filing. Attorney addresses cluster near immigration courts,
#     potentially inflating geographic concentration in court cities.
#   - ZCTAs (ZIP Code Tabulation Areas) are Census constructs, not identical to
#     USPS ZIP codes. PO Box-only ZIPs, military ZIPs (APO/FPO), and ZIPs
#     created after 2020 have no ZCTA match and will be NA after joining.
#   - County and place assignment uses POPULATION-WEIGHTED allocation from
#     geocorr (largest population overlap). For ZCTAs straddling boundaries,
#     the county/place with the highest population share is assigned.
#   - Territories (AS, GU, MP, VI) are not in geocorr; for these, county and
#     place are sourced from Census 2020 relationship files using area-weighted
#     allocation.
#   - 2020 Census geography is applied to all cases regardless of year. Match
#     rates may degrade for older cases due to ZIP code churn.

# --- State FIPS → abbreviation lookup from geocorr state file ---
state_lookup <- read_csv(
  "inputs/geocorr2022_2607607974_state.csv"
) |>
  slice(-1) |>
  type_convert() |>
  distinct(state, stab) |>
  transmute(
    state_fips_code = str_pad(state, 2, pad = "0"),
    state = stab
  )

# --- ZIP → state + county from geocorr (ZCTA → county) ---
# County file has: zcta, county (5-digit FIPS), CountyName ("Name ST")
# State and state_fips_code derived from county FIPS; state abbreviation from state file.
zcta_county <- read_csv(
  "inputs/geocorr2022_2607608761_county.csv"
) |>
  slice(-1) |>
  type_convert() |>
  filter(!is.na(zcta), str_detect(zcta, "^\\d+$")) |>
  group_by(zcta) |>
  slice_max(afact, n = 1, with_ties = FALSE) |>
  ungroup() |>
  mutate(
    county_fips_code = str_pad(county, 5, pad = "0"),
    state_fips_code = str_sub(county_fips_code, 1, 2)
  ) |>
  left_join(state_lookup, by = "state_fips_code") |>
  transmute(
    zcta,
    state,
    state_fips_code,
    county = str_remove(CountyName, ",? [A-Z]{2}(\\s*\\(.*\\))?$") |>
      str_squish(),
    county_fips_code
  )

# --- ZIP → place from geocorr (ZCTA → place) ---
zcta_place <- read_csv(
  "inputs/geocorr2022_2607609986_place.csv"
) |>
  slice(-1) |>
  type_convert() |>
  filter(!is.na(zcta), str_detect(zcta, "^\\d+$")) |>
  group_by(zcta) |>
  slice_max(afact, n = 1, with_ties = FALSE) |>
  ungroup() |>
  rename(place_fips_code = place) |>
  transmute(
    zcta,
    place = if_else(
      place_fips_code == "99999",
      NA_character_,
      PlaceName |>
        str_remove(",? [A-Z]{2}$") |>
        str_remove("\\s*\\(.*\\)") |>
        str_squish()
    ),
    place_fips_code = na_if(place_fips_code, "99999")
  )

# --- Territory supplement from Census 2020 relationship files (AS, GU, MP, VI) ---
# Geocorr covers 50 states + DC + PR; territories need Census relationship files.
# These use area-weighted allocation (AREALAND_PART).
territory_fips <- c("60", "66", "69", "78")
territory_state_lookup <- tribble(
  ~state_fips_code , ~state ,
  "60"        , "AS"   ,
  "66"        , "GU"   ,
  "69"        , "MP"   ,
  "78"        , "VI"
)

territory_county <- read_delim(
  "inputs/tab20_zcta520_county20_natl.txt",
  delim = "|",
  locale = locale(encoding = "latin1")
) |>
  filter(
    !is.na(GEOID_ZCTA5_20),
    str_sub(GEOID_COUNTY_20, 1, 2) %in% territory_fips
  ) |>
  group_by(GEOID_ZCTA5_20) |>
  slice_max(AREALAND_PART, n = 1, with_ties = FALSE) |>
  ungroup() |>
  transmute(
    zcta = GEOID_ZCTA5_20,
    state_fips_code = str_sub(GEOID_COUNTY_20, 1, 2),
    county = str_remove(
      NAMELSAD_COUNTY_20,
      " (County|Borough|Census Area|Municipality|District|Island|Municipio)$"
    ),
    county_fips_code = GEOID_COUNTY_20
  ) |>
  left_join(territory_state_lookup, by = "state_fips_code")

territory_place <- read_delim(
  "inputs/tab20_zcta520_place20_natl.txt",
  delim = "|",
  locale = locale(encoding = "latin1")
) |>
  filter(
    !is.na(GEOID_ZCTA5_20),
    str_sub(GEOID_PLACE_20, 1, 2) %in% territory_fips
  ) |>
  group_by(GEOID_ZCTA5_20) |>
  slice_max(AREALAND_PART, n = 1, with_ties = FALSE) |>
  ungroup() |>
  transmute(
    zcta = GEOID_ZCTA5_20,
    place = NAMELSAD_PLACE_20 |>
      str_remove("\\s*\\(.*\\)") |>
      str_squish(),
    place_fips_code = str_sub(GEOID_PLACE_20, 3, 7)
  )

territory_lookup <- left_join(territory_county, territory_place, by = "zcta") |>
  select(zcta, state, state_fips_code, county, county_fips_code, place, place_fips_code)

# --- Combine geocorr + territory lookups ---
zip_lookup <-
  left_join(zcta_county, zcta_place, by = "zcta") |>
  bind_rows(territory_lookup)

# --- Validation checks ---

us_50_plus_dc <- c(state.abb, "DC")
territories <- c("AS", "GU", "MP", "PR", "VI")

zip_lookup |>
  col_vals_not_null(zcta) |>
  col_vals_not_null(state) |>
  col_vals_not_null(state_fips_code) |>
  col_vals_not_null(county) |>
  col_vals_not_null(county_fips_code) |>
  col_vals_regex(zcta, "^\\d{5}$") |>
  col_vals_regex(state_fips_code, "^\\d{2}$") |>
  col_vals_regex(county_fips_code, "^\\d{5}$") |>
  rows_distinct(zcta) |>
  # County and place names should not end with state abbreviations
  col_vals_expr(
    ~ !str_detect(county, ",? [A-Z]{2}$"),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_expr(
    ~ is.na(place) | !str_detect(place, ",? [A-Z]{2}$"),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_in_set(
    state,
    c(us_50_plus_dc, territories),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  col_vals_between(
    n_states,
    left = 51,
    right = 56,
    preconditions = \(x) tibble(n_states = n_distinct(x$state))
  ) |>
  # 2020 Census has ~33,120 ZCTAs
  col_vals_between(
    n_zcta,
    left = 32000,
    right = 34000,
    preconditions = \(x) tibble(n_zcta = n_distinct(x$zcta))
  ) |>
  invisible()

arrow::write_parquet(zip_lookup, "tmp/zip_lookup.parquet")
