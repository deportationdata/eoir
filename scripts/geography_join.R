library(tidyverse)
library(tidylog)
library(pointblank)

# --- ZIP → county, state lookup for EOIR respondent geography ---
#
# The ZCTA → county/place allocation itself is computed and published by
# deportationdata/us-shapefiles (code/zip_xwalk.R there), not here — this
# script only fetches that file and renames it to this pipeline's schema.
# Fetching (not vendoring a copy of the input CSVs) means an upstream fix
# there — e.g. a geocorr vintage refresh or an encoding correction — reaches
# this pipeline on the next run without a code change here.
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
#   - place_fips_code is the full 7-digit Census place GEOID: 2-digit state
#     FIPS + 5-digit place code. County and place are assigned independently
#     (each by largest population share), so a ZCTA straddling a state line
#     can have its place in a different state than its county; the GEOID's
#     state prefix follows the place's own state and may differ from
#     state_fips_code (which follows the county). In the 2022 geocorr vintage
#     this affects exactly one ZCTA (97635: county Modoc, CA 06; place New
#     Pine Creek CDP, OR 41) — correct behavior, not a join error.

ZIP_XWALK_URL <- paste0(
  "https://media.githubusercontent.com/media/deportationdata/",
  "us-shapefiles/main/data/zip-xwalk.parquet"
)
zip_xwalk_path <- "tmp/zip-xwalk.parquet"

# media.githubusercontent.com/media/... resolves the Git LFS object; the
# plain raw.githubusercontent.com URL for an LFS-tracked file instead
# returns ~130 bytes of pointer text ("version https://git-lfs...") that
# arrow would fail to parse anyway, but check the byte count directly so a
# bad URL fails with a clear message instead of a cryptic parquet error.
download.file(ZIP_XWALK_URL, zip_xwalk_path, mode = "wb", quiet = TRUE)
stopifnot(
  "zip-xwalk.parquet download looks like a Git LFS pointer, not real data — check ZIP_XWALK_URL uses media.githubusercontent.com/media/..., not raw.githubusercontent.com" =
    file.size(zip_xwalk_path) > 100000
)

zip_xwalk <- arrow::read_parquet(zip_xwalk_path)

# us-shapefiles' own column names (see its README) differ from this
# pipeline's long-standing schema; rename rather than touch every
# downstream reference in eoir_case_joins.R.
zip_lookup <-
  zip_xwalk |>
  transmute(
    zcta,
    state = stusps,
    state_fips_code = str_sub(county_geoid, 1, 2),
    county,
    county_fips_code = county_geoid,
    place,
    place_fips_code = place_geoid
  )

# --- Validation checks ---
# Same checks this script ran on its own locally-computed zip_lookup before
# the fetch — kept (not relaxed) because this data now arrives from a
# network fetch of another repo's build output rather than a local
# computation this script controls directly.

us_50_plus_dc <- c(state.abb, "DC")
territories <- c("AS", "GU", "MP", "PR", "VI")

# Non-ASCII string literals in .R source parse with Encoding() "unknown", not
# "UTF-8" — in a non-UTF-8 session locale (e.g. a CI runner set to C), `==`
# against an arrow-decoded (Encoding() "UTF-8") string then compares unequal
# byte-for-byte-identical text. Tag the literal explicitly so the encoding
# canary below doesn't depend on the build machine's locale.
utf8_literal <- function(x) {
  Encoding(x) <- "UTF-8"
  x
}

zip_lookup |>
  col_vals_not_null(zcta) |>
  col_vals_not_null(state) |>
  col_vals_not_null(state_fips_code) |>
  col_vals_not_null(county) |>
  col_vals_not_null(county_fips_code) |>
  col_vals_regex(zcta, "^\\d{5}$") |>
  col_vals_regex(state_fips_code, "^\\d{2}$") |>
  col_vals_regex(county_fips_code, "^\\d{5}$") |>
  col_vals_regex(place_fips_code, "^\\d{7}$", na_pass = TRUE) |>
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
  # Encoding guards: names must be valid UTF-8 with no U+FFFD replacement
  # character (U+FFFD would appear if us-shapefiles' own build ever misread
  # a latin1 geocorr file as UTF-8 again)
  col_vals_expr(~ validUTF8(county) & !str_detect(county, "\uFFFD")) |>
  col_vals_expr(
    ~ is.na(place) | (validUTF8(place) & !str_detect(place, "\uFFFD"))
  ) |>
  # Encoding canary: at least one PR municipio must retain its diacritic
  # ("Añasco Municipio"). Catches mojibake regressions upstream that the
  # U+FFFD check alone cannot see.
  col_vals_gte(
    n_anasco,
    1,
    preconditions = \(x) {
      tibble(n_anasco = sum(x$county == utf8_literal("Añasco Municipio")))
    }
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
