# The six per-application-type decision columns in eoir_court_applications.R
# used to be one summarise() holding
#   dplyr::last(appl_dec[appl_code %in% "ASYL"])
# and five siblings. DuckDB cannot translate `[`, so duckplyr fell back to
# dplyr and evaluated the subset once per case in R — about 15 minutes on the
# real 16.1M-row table. They are now six filter + summarise + left_join steps
# that DuckDB can execute (~400x faster).
#
# That is a real restructuring of how a published column is derived, so this
# pins the new form against the old one on the cases that actually differ:
# missing idncase, missing decisions, cases with no application of a type, and
# same-day applications resolved by the dec_rank tie-break.
#
# Also covers fast_convert()'s datetime branch, which moved from as.POSIXct()
# to readr::parse_datetime(). fast_convert() counts new NAs as parse failures
# and aborts above max_fail_rate, so a parser that disagrees about what fails
# would change both the data and the abort behaviour.
#
# Run: Rscript tests/test_court_applications_decisions.R

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
  library(readr)
})

source("scripts/utilities.R")

failures <- character()
check <- function(label, ok) {
  if (isTRUE(ok)) {
    cat("  ok   -", label, "\n")
  } else {
    failures <<- c(failures, label)
    cat("  FAIL -", label, "\n")
  }
}

# ---------------------------------------------------------------- fixture ---
set.seed(11)
N <- 60000
G <- 9000
court_applications_tbl <- data.frame(
  # deliberately include NA idncase rows: the old code dropped them after the
  # aggregate, the new one excludes them before the joins
  idncase = c(sample(seq_len(G), N - 4L, replace = TRUE), NA, NA, NA, NA),
  appl_recd_day = as.Date("2005-01-01") + sample(0:4000, N, replace = TRUE),
  dec_rank = sample(0:14, N, replace = TRUE),
  appl_recd_date = as.Date("2005-01-01") + sample(0:4000, N, replace = TRUE),
  idnproceedingappln = seq_len(N),
  appl_code = sample(
    c("ASYL", "ASYW", "WCAT", "245", "42B", "42A", "OTHER"),
    N,
    replace = TRUE
  ),
  # NA decisions must survive as NA, not be confused with "no such application"
  appl_dec = sample(c("F", "G", "D", "C", "W", NA), N, replace = TRUE),
  stringsAsFactors = FALSE
)
# a case whose applications all land on the same day, so only dec_rank orders them
court_applications_tbl$appl_recd_day[court_applications_tbl$idncase %in% 1] <-
  as.Date("2010-06-01")
# a case with no ASYL application at all
court_applications_tbl$appl_code[court_applications_tbl$idncase %in% 2] <- "OTHER"

court_applications_tbl <- court_applications_tbl |>
  arrange(
    idncase,
    desc(is.na(appl_recd_day)),
    appl_recd_day,
    dec_rank,
    appl_recd_date,
    idnproceedingappln
  )

# ------------------------------------------------- reference (old form) ------
reference <- court_applications_tbl |>
  summarise(
    asylum_decision_last = dplyr::last(appl_dec[appl_code %in% "ASYL"]),
    withholding_decision_last = dplyr::last(appl_dec[appl_code %in% "ASYW"]),
    cat_decision_last = dplyr::last(appl_dec[appl_code %in% "WCAT"]),
    adjustment_decision_last = dplyr::last(appl_dec[appl_code %in% "245"]),
    non_lpr_cancellation_decision_last = dplyr::last(
      appl_dec[appl_code %in% "42B"]
    ),
    lpr_cancellation_decision_last = dplyr::last(
      appl_dec[appl_code %in% "42A"]
    ),
    .by = idncase
  ) |>
  arrange(idncase) |>
  filter(!is.na(idncase)) |>
  as.data.frame()

# --------------------------------------------------- current implementation --
last_decision_for <- function(appl_code_value, column_name) {
  court_applications_tbl |>
    filter(appl_code %in% appl_code_value) |>
    summarise("{column_name}" := dplyr::last(appl_dec), .by = idncase)
}

current <- court_applications_tbl |>
  distinct(idncase) |>
  filter(!is.na(idncase)) |>
  left_join(last_decision_for("ASYL", "asylum_decision_last"), by = "idncase") |>
  left_join(
    last_decision_for("ASYW", "withholding_decision_last"),
    by = "idncase"
  ) |>
  left_join(last_decision_for("WCAT", "cat_decision_last"), by = "idncase") |>
  left_join(
    last_decision_for("245", "adjustment_decision_last"),
    by = "idncase"
  ) |>
  left_join(
    last_decision_for("42B", "non_lpr_cancellation_decision_last"),
    by = "idncase"
  ) |>
  left_join(
    last_decision_for("42A", "lpr_cancellation_decision_last"),
    by = "idncase"
  ) |>
  arrange(idncase) |>
  as.data.frame()

reference <- reference[order(reference$idncase), ]
current <- current[order(current$idncase), names(reference)]
rownames(reference) <- NULL
rownames(current) <- NULL

cat("\n[1/3] decision columns match the previous implementation\n")
check("identical to reference", isTRUE(all.equal(reference, current, check.attributes = FALSE)))
check("same number of cases", nrow(reference) == nrow(current))
check("NA idncase excluded", !any(is.na(current$idncase)))
check("one row per case", !any(duplicated(current$idncase)))
check(
  "cases with no application of a type are NA",
  any(is.na(current$asylum_decision_last))
)
for (cn in setdiff(names(reference), "idncase")) {
  check(
    sprintf("column %s identical", cn),
    identical(reference[[cn]], current[[cn]])
  )
}

cat("\n[2/3] fast_convert() datetime branch matches as.POSIXct()\n")
dt_inputs <- c(
  "2020-01-01 00:00:00", "1999-12-31 23:59:59", "2020-06-15 12:30:45",
  "", NA, "not a date", "2020-13-45 99:99:99", "1000", "N/A",
  "2020-01-01", "  2020-01-01 00:00:00  "
)
old_parse <- as.POSIXct(dt_inputs, format = "%Y-%m-%d %H:%M:%S", tz = "UTC")
converted <- fast_convert(
  data.frame(d = dt_inputs, stringsAsFactors = FALSE),
  list(d = "datetime"),
  max_fail_rate = 1
)$d
check("NA pattern identical", identical(is.na(old_parse), is.na(converted)))
check(
  "parsed values identical",
  isTRUE(all.equal(as.numeric(old_parse), as.numeric(converted)))
)
check("class is POSIXct", inherits(converted, "POSIXct"))
check("timezone is UTC", identical(attr(converted, "tzone"), "UTC"))
check("no readr `problems` attribute left behind", is.null(attr(converted, "problems")))
check(
  "as.Date() downstream agrees",
  identical(as.Date(old_parse, tz = "UTC"), as.Date(converted, tz = "UTC"))
)
check(
  "a bare date still fails to parse, as before",
  is.na(converted[dt_inputs %in% "2020-01-01"])
)

cat("\n[3/3] fast_convert() still aborts on excessive parse failures\n")
bad <- data.frame(d = c(rep("2020-01-01 00:00:00", 90), rep("garbage", 10)),
                  stringsAsFactors = FALSE)
threw <- tryCatch(
  {
    fast_convert(bad, list(d = "datetime"), max_fail_rate = 0.001)
    FALSE
  },
  error = function(e) grepl("parse failures exceed", conditionMessage(e))
)
check("errors when failures exceed max_fail_rate", threw)
under <- tryCatch(
  {
    fast_convert(bad, list(d = "datetime"), max_fail_rate = 0.5)
    TRUE
  },
  error = function(e) FALSE
)
check("passes when under max_fail_rate", under)

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d court-applications check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All court-applications / fast_convert checks passed.\n")
