# eoir_case_joins.R carries the widest frame in the pipeline (~180 columns), so
# any verb DuckDB cannot translate drops the whole thing back into R and copies
# it. Two did: tidyr's replace_na() and dplyr's recode_values(). On a
# 400k x 194 fixture they cost +1812MB and +1254MB of R heap, and gc() did not
# give it back. On the real table that was ~35GB, and the runner — which has
# 63GB and was also hosting DuckDB — was killed with machine memory at
# 64,287 of 64,297MB.
#
# The replacements are exact, not approximate:
#   replace_na(x, v)      -> coalesce(x, v)          identical on character
#   recode_values(x, ...) -> left_join(labels)       both give NA for a code
#                                                    that matches nothing, and
#                                                    for NA itself
# Same fixture after the swap: +0MB.
#
# Values are unchanged either way, so nothing about the output would reveal a
# regression here — only the clock and the memory. Hence this test.
#
# Run: Rscript tests/test_case_joins_native.R

Sys.setenv(DUCKPLYR_FALLBACK_AUTOUPLOAD = "0")
suppressPackageStartupMessages({
  library(dplyr)
  library(duckplyr)
  library(tidyr)
})

failures <- character()
check <- function(label, ok) {
  if (isTRUE(ok)) {
    cat("  ok   -", label, "\n")
  } else {
    failures <<- c(failures, label)
    cat("  FAIL -", label, "\n")
  }
}

cat("\n[1/5] coalesce() is a drop-in for replace_na()\n")
x <- c("DENY", NA, "GRANT", "")
check(
  "identical on a character column, empty string included",
  identical(replace_na(x, "No application"), coalesce(x, "No application"))
)

cat("\n[2/5] a label join is a drop-in for recode_values()\n")
set.seed(7)
n <- 50000
codes <- data.frame(
  id = seq_len(n),
  # "Z" matches nothing, and NA must stay NA
  custody_code = sample(c("N", "R", "D", "Z", NA), n, replace = TRUE),
  stringsAsFactors = FALSE
)
labels <- data.frame(
  custody_code = c("N", "R", "D"),
  custody = c("never detained", "released", "detained throughout"),
  stringsAsFactors = FALSE
)
recoded <- codes |>
  mutate(
    custody = recode_values(
      custody_code,
      "N" ~ "never detained",
      "R" ~ "released",
      "D" ~ "detained throughout"
    )
  ) |>
  arrange(id)
joined <- codes |>
  left_join(labels, by = "custody_code", relationship = "many-to-one") |>
  arrange(id)

check("same number of rows (the join does not fan out)", nrow(recoded) == nrow(joined))
check("same labels", identical(recoded$custody, joined$custody))
check(
  "an unmatched code becomes NA under both",
  all(is.na(joined$custody[joined$custody_code %in% "Z"]))
)
check(
  "the fixture actually exercises unmatched and NA codes",
  any(codes$custody_code %in% "Z") && any(is.na(codes$custody_code))
)

cat("\n[3/5] the wide-frame script uses neither\n")
src <- readLines("scripts/eoir_case_joins.R", warn = FALSE)
src <- src[!grepl("^\\s*#", src)]
for (verb in c("replace_na", "recode_values")) {
  hits <- grep(verb, src, value = TRUE)
  check(sprintf("eoir_case_joins.R free of %s()", verb), length(hits) == 0)
  for (l in utils::head(hits, 2)) cat("         ", trimws(l), "\n")
}

cat("\n[4/5] row_count_match() copes with a lazy table\n")
# nrow() is NA on a dbplyr handle, which made this helper fail with "missing
# value where TRUE/FALSE needed" once a validation chain ran against DuckDB.
source("scripts/utilities.R")
con <- DBI::dbConnect(duckdb::duckdb())
on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)
duckdb::duckdb_register(con, "rcm_tbl", data.frame(a = 1:2500))
lazy <- dplyr::tbl(con, "rcm_tbl")

check("nrow() really is NA for the lazy table (the trap)", is.na(nrow(lazy)))
check(
  "matching count passes",
  tryCatch({ row_count_match(lazy, 2500L); TRUE }, error = function(e) FALSE)
)
check(
  "mismatched count still raises, and says the counts",
  tryCatch({ row_count_match(lazy, 9L); FALSE },
           error = function(e) grepl("expected 9 rows, got 2500", conditionMessage(e)))
)

cat("\n[5/6] validate_in_duckdb() hands over the frame as it actually is\n")
# The tidier-looking bridge would be duckplyr's own relation via last_rel(),
# but that returns a stale relation: on a frame built with filter() then
# mutate() it reports only the pre-mutate columns. Validating it would check
# the wrong data and say nothing. compute_parquet() reflects the real frame.
lz <- duckplyr::as_duckdb_tibble(
  data.frame(a = 1:20000, b = sample(letters, 20000, replace = TRUE),
             stringsAsFactors = FALSE),
  prudence = "lavish"
) |>
  dplyr::filter(a > 10) |>
  dplyr::mutate(flag = a %% 2L, derived = paste0(b, "!"))

stale <- tryCatch(
  {
    duckdb:::rel_to_view(duckplyr::last_rel(), "main", "stale_v", TRUE)
    colnames(dplyr::tbl(con, "stale_v"))
  },
  error = function(e) NA_character_
)
check(
  "last_rel() really is stale (why we do not use it)",
  isTRUE(is.na(stale[1])) || !all(c("flag", "derived") %in% stale)
)

seen <- NULL
validate_in_duckdb(lz, function(tbl) { seen <<- colnames(tbl); invisible(tbl) })
check("post-mutate columns reach the validation", all(c("flag", "derived") %in% seen))
check("no column is lost", setequal(seen, colnames(lz)))
check("temp handoff files are cleaned up", length(list.files("tmp", "^validate_")) == 0)

cat("\n[6/6] the title-case loop matches the across() it replaces\n")
# across() builds every rewritten column before assigning any, which on the
# ~180-column cases frame held ~100 rewritten character columns alongside the
# originals and took R past 61GB. The loop is only safe if it selects exactly
# the same columns and produces exactly the same values.
is.POSIXct <- function(x) inherits(x, "POSIXct")
set.seed(4)
nn <- 5000
fixture <- data.frame(
  idncase = seq_len(nn),
  language = sample(c("SPANISH", "bia review", NA, "v/d granted"), nn, TRUE),
  nationality = sample(c("MEXICO", "el salvador", NA), nn, TRUE),
  first_court_code = "LOS",
  first_court = sample(c("LOS ANGELES (LOS)", "new york (NYC)"), nn, TRUE),
  final_court = "MIAMI (MIA)", bond_court_first = "DALLAS (DAL)",
  bond_court_second = "DENVER (DEN)", bond_court_last = "BOSTON (BOS)",
  first_judge_name = "SMITH, JOHN", charge_section_1 = "212(a)(6)(C)(i)",
  county_fips_code = "06037", state = "CA",
  dt = as.POSIXct("2020-01-02 03:04:05", tz = "UTC") + seq_len(nn),
  stringsAsFactors = FALSE
)
AB <- c("ABC", "BIA", "CAT", "CFV", "CPC", "DHS", "EOIR", "IJ", "INA", "ORR",
        "PD", "ROP", "V/D", "VD", "WCAT")
courts <- c("first_court", "final_court", "bond_court_first",
            "bond_court_second", "bond_court_last")

by_across <- fixture |>
  dplyr::mutate(
    dplyr::across(where(is.POSIXct), ~ as.Date(.x, tz = "UTC")),
    dplyr::across(
      where(is.character) & !contains("_code") & !contains("_court") &
        !contains("judge_name") & !contains("charge_section") &
        !contains("fips") & !contains("state"),
      ~ stringr::str_to_title(.x) |> str_fix_abbreviations(abbr = AB)
    ),
    dplyr::across(dplyr::all_of(courts),
      ~ stringr::str_replace(.x, "^([^(]+)", \(m) stringr::str_to_title(m)))
  )

EX <- c("_code", "_court", "judge_name", "charge_section", "fips", "state")
by_loop <- fixture
sel <- names(by_loop)[vapply(by_loop, is.character, logical(1)) &
  !Reduce(`|`, lapply(EX, \(p) grepl(p, names(by_loop), fixed = TRUE)))]
for (nm in sel) {
  by_loop[[nm]] <- str_fix_abbreviations(stringr::str_to_title(by_loop[[nm]]), abbr = AB)
}
by_loop <- by_loop |>
  dplyr::mutate(
    dplyr::across(where(is.POSIXct), ~ as.Date(.x, tz = "UTC")),
    dplyr::across(dplyr::all_of(courts),
      ~ stringr::str_replace(.x, "^([^(]+)", \(m) stringr::str_to_title(m)))
  )

check("selects the same columns as the tidyselect expression",
      identical(sel, c("language", "nationality")))
check("identical output", isTRUE(all.equal(as.data.frame(by_across), as.data.frame(by_loop))))
check("abbreviations survive title casing",
      any(grepl("BIA", by_loop$language, fixed = TRUE)) &&
        any(grepl("V/D", by_loop$language, fixed = TRUE)))
check("court codes stay upper case inside parentheses",
      all(grepl("\\([A-Z]{3}\\)$", by_loop$first_court)))
check("excluded columns are untouched",
      identical(by_loop$first_judge_name, fixture$first_judge_name) &&
        identical(by_loop$state, fixture$state))

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d case-joins check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All case-joins native-verb checks passed.\n")
