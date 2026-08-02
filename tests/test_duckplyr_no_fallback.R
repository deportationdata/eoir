# Guardrail: fail loudly if the pipeline's hot-path dplyr verbs stop running
# inside DuckDB.
#
# duckplyr falls back to plain dplyr *silently* when it meets something it
# cannot translate. The results stay correct, so nothing breaks visibly -- the
# pipeline just quietly returns to pre-migration speed. That is exactly how the
# original group_by() regression went unnoticed: every case-level collapse was
# running in R while appearing to work fine.
#
# Two independent mechanisms are used here:
#   * prudence = "stingy"  -- errors on unsupported *verbs* (group_by, rowwise,
#     grouped filter, slice_max, ...)
#   * DUCKPLYR_FORCE=TRUE  -- errors on untranslatable *functions* (case_when,
#     paste0, stringr, custom R functions, ...)
#
# The second block additionally pins the collapse semantics against a
# data.table reference: `arrange()` then bare `first()`/`last()` relies on
# DuckDB preserving input order into the aggregate, which duckplyr does not
# formally guarantee ("DuckDB does not guarantee order stability for the
# output"). It holds today; this test is what catches it if an upgrade changes
# that.
#
# Run: Rscript tests/test_duckplyr_no_fallback.R

Sys.setenv(DUCKPLYR_FALLBACK_AUTOUPLOAD = "0")  # never phone home from CI
suppressPackageStartupMessages({
  library(dplyr)
  library(duckplyr)
})

failures <- character()
ok <- function(msg) cat("  ok   -", msg, "\n")
bad <- function(msg, detail = "") {
  failures <<- c(failures, msg)
  cat("  FAIL -", msg, if (nzchar(detail)) paste0(" (", detail, ")") else "", "\n")
}

# A frame shaped like the per-table inputs: a case id, an ordering key, a
# tie-break, and payload columns.
set.seed(1)
n <- 20000
fixture <- data.frame(
  idncase = sample(1:2000, n, replace = TRUE),
  comp_date = as.Date("2005-01-01") + sample(0:4000, n, replace = TRUE),
  seq_id = seq_len(n),
  code = sample(c("RMV", "WHO", "DEP"), n, replace = TRUE),
  appl_code = sample(c("ASYL", "ASYW", "WCAT", "OTHER"), n, replace = TRUE),
  appl_dec = sample(c("F", "G", "D"), n, replace = TRUE),
  stringsAsFactors = FALSE
)

stingy <- function() duckplyr::as_duckdb_tibble(fixture, prudence = "stingy")

# DUCKPLYR_FORCE turns a fallback into an error. Verified to take effect when
# set after the package is loaded, and to catch unsupported verbs as well as
# untranslatable functions.
with_force <- function(f) {
  old <- Sys.getenv("DUCKPLYR_FORCE", unset = NA)
  Sys.setenv(DUCKPLYR_FORCE = "TRUE")
  on.exit(
    if (is.na(old)) Sys.unsetenv("DUCKPLYR_FORCE") else Sys.setenv(DUCKPLYR_FORCE = old),
    add = TRUE
  )
  invisible(as.data.frame(f(duckplyr::as_duckdb_tibble(fixture))))
  TRUE
}

# Each entry is a shape the pipeline actually relies on. If any of these stops
# being executable by DuckDB, the pipeline silently loses its speedup.
shapes <- list(
  "arrange + summarise(.by=) first/last" = function(d) {
    d |> arrange(idncase, comp_date, seq_id) |>
      summarise(a = first(code), b = last(code), .by = idncase)
  },
  "summarise(.by=) nth() positional" = function(d) {
    d |> arrange(idncase, comp_date, seq_id) |>
      summarise(a = nth(code, 1), b = nth(code, 2), .by = idncase)
  },
  "n_distinct aggregate + inner_join (eoir_proceeding)" = function(d) {
    keep <- d |> summarise(k = n_distinct(code), .by = idncase) |>
      filter(k == 1) |> select(idncase)
    d |> inner_join(keep, by = "idncase")
  },
  "in_absentia plain boolean" = function(d) {
    d |> mutate(z = !is.na(code) & code == "RMV")
  },
  "arrange + NA-first ordering (eoir_court_applications)" = function(d) {
    d |> arrange(idncase, desc(is.na(comp_date)), comp_date, seq_id)
  },
  # Was a known fallback: last(appl_dec[appl_code %in% "ASYL"]) cannot be
  # translated because DuckDB has no `[`. Filtering to one application type
  # first leaves a plain last() that DuckDB executes, which is what made
  # eoir_court_applications.R ~15 minutes faster. If this ever stops being
  # native the pipeline silently loses that back.
  "filter + summarise(.by=) per application type" = function(d) {
    d |> arrange(idncase, comp_date, seq_id) |>
      filter(appl_code %in% "ASYL") |>
      summarise(asylum_decision_last = dplyr::last(appl_dec), .by = idncase)
  }
)

cat("\n[1/4] unsupported-verb check (prudence = \"stingy\")\n")
for (nm in names(shapes)) {
  res <- tryCatch({
    invisible(as.data.frame(shapes[[nm]](stingy())))
    TRUE
  }, error = function(e) conditionMessage(e))
  if (isTRUE(res)) ok(nm) else bad(nm, gsub("\\s+", " ", substr(res, 1, 90)))
}

cat("\n[2/4] untranslatable-function check (DUCKPLYR_FORCE=TRUE)\n")
for (nm in names(shapes)) {
  res <- tryCatch(
    with_force(shapes[[nm]]),
    error = function(e) conditionMessage(e)
  )
  if (isTRUE(res)) ok(nm) else bad(nm, gsub("\\s+", " ", substr(res, 1, 90)))
}

cat("\n[3/4] known, accepted fallbacks (reported, not failed)\n")
# These genuinely cannot run in DuckDB. They are listed so the boundary stays
# visible: if one ever *starts* translating, that is a free speedup worth
# promoting into the must-be-native list above.
#
# The conditional aggregate that used to live here has been promoted: it is
# now expressed as filter + summarise per application type and is required to
# be native.
known_fallbacks <- list(
  "summarise(.by=) conditional last(x[cond]) - no longer used in the pipeline" = function(d) {
    d |> arrange(idncase, comp_date, seq_id) |>
      summarise(a = dplyr::last(appl_dec[appl_code %in% "ASYL"]), .by = idncase)
  }
)
for (nm in names(known_fallbacks)) {
  res <- tryCatch({
    invisible(as.data.frame(known_fallbacks[[nm]](stingy())))
    "now NATIVE - consider promoting to the must-be-native list"
  }, error = function(e) "still falls back (expected)")
  cat("  note -", nm, "--", res, "\n")
}

cat("\n[4/4] collapse semantics still match a data.table reference\n")
if (requireNamespace("data.table", quietly = TRUE)) {
  dt <- data.table::as.data.table(fixture)
  data.table::setorder(dt, idncase, comp_date, seq_id)
  reference <- as.data.frame(
    dt[, list(a = data.table::first(code), b = data.table::last(code)), by = idncase]
  )
  reference <- reference[order(reference$idncase), ]
  rownames(reference) <- NULL

  got <- duckplyr::as_duckdb_tibble(fixture) |>
    arrange(idncase, comp_date, seq_id) |>
    summarise(a = first(code), b = last(code), .by = idncase) |>
    arrange(idncase) |>
    as.data.frame()
  rownames(got) <- NULL

  if (isTRUE(all.equal(reference, got, check.attributes = FALSE))) {
    ok("first()/last() match data.table after arrange()")
  } else {
    bad("first()/last() DIVERGED from data.table after arrange()",
        "DuckDB may have stopped preserving input order into aggregates")
  }
} else {
  cat("  skip - data.table not installed\n")
}

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d duckplyr guardrail check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All duckplyr guardrail checks passed.\n")
