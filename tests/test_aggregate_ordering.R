# Guards the ordering contract that every case-level collapse depends on.
#
# DuckDB does not promise to feed rows to an aggregate in input order:
#
#   "GROUP BY (neither in- nor output order are guaranteed)"
#   "Whole-table aggregation (the input order ... is not guaranteed unless
#    explicitly specified in the aggregate function)"
#   -- https://duckdb.org/docs/stable/sql/dialect/order_preservation
#
# So `arrange(...) |> summarise(last(x), .by = g)` is NOT safe, even though it
# looks right and passes casual testing. It really does return the wrong row
# for a small number of groups, and a *different* number of them on each run —
# on a 400k-row fixture this was 2, 2, then 1 wrong out of ~70,000 groups.
# That is silently miscomputed case data, not a formatting quirk.
#
# The fix, which is what DuckDB's own docs recommend, is to freeze the sort
# order into a column with row_number() and pass it to every order-sensitive
# aggregate as order_by=. Each pipeline script does this immediately after its
# arrange(); this test pins the property so the pattern cannot quietly rot.
#
# Run: Rscript tests/test_aggregate_ordering.R

Sys.setenv(DUCKPLYR_FALLBACK_AUTOUPLOAD = "0")
suppressPackageStartupMessages({
  library(dplyr)
  library(duckplyr)
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
# NA-safe difference count: NA vs NA is a match, NA vs value is not
n_diff <- function(a, b) {
  sum((is.na(a) != is.na(b)) | (!is.na(a) & !is.na(b) & a != b))
}

# Adversarial on purpose: multi-key sort, an NA-first key needing desc(),
# heavy tie density, and groups short enough that nth(3)/nth(4) run past the
# end. Large enough that DuckDB parallelises the aggregate.
set.seed(21)
N <- 400000
G <- 70000
fixture <- data.frame(
  g = sample(seq_len(G), N, replace = TRUE),
  d1 = sample(c(as.Date("2005-01-01") + 0:400, NA), N, replace = TRUE),
  rank2 = sample(0:4, N, replace = TRUE),
  id3 = sample(N),
  x = sprintf("v%06d", sample(seq_len(999999), N, replace = TRUE)),
  stringsAsFactors = FALSE
)
sorted <- fixture |> arrange(g, desc(is.na(d1)), d1, rank2, id3)

# Ground truth computed in plain R from the sorted frame — no duckplyr.
by_group <- split(sorted$x, sorted$g)
nth_or_na <- function(v, i) if (length(v) >= i) v[i] else NA_character_
truth <- data.frame(
  g = as.integer(names(by_group)),
  first_x = vapply(by_group, nth_or_na, "", 1L),
  last_x = vapply(by_group, function(v) v[length(v)], ""),
  nth2_x = vapply(by_group, nth_or_na, "", 2L),
  nth4_x = vapply(by_group, nth_or_na, "", 4L),
  stringsAsFactors = FALSE
)

collapse_with_order <- function() {
  duckplyr::as_duckdb_tibble(fixture) |>
    arrange(g, desc(is.na(d1)), d1, rank2, id3) |>
    mutate(row_order = row_number()) |>
    summarise(
      first_x = first(x, order_by = row_order),
      last_x = last(x, order_by = row_order),
      nth2_x = nth(x, 2, order_by = row_order),
      nth4_x = nth(x, 4, order_by = row_order),
      .by = g
    ) |>
    as.data.frame()
}

cat("\n[1/3] explicit order_by = row_order matches plain-R truth\n")
for (rep in 1:3) {
  got <- merge(truth, collapse_with_order(), by = "g", suffixes = c(".t", ".g"))
  check(
    sprintf("run %d: first/last/nth all exact", rep),
    n_diff(got$first_x.t, got$first_x.g) == 0 &&
      n_diff(got$last_x.t, got$last_x.g) == 0 &&
      n_diff(got$nth2_x.t, got$nth2_x.g) == 0 &&
      n_diff(got$nth4_x.t, got$nth4_x.g) == 0
  )
}
check(
  "fixture exercises nth() past the end of short groups",
  any(is.na(truth$nth4_x))
)

cat("\n[2/3] the aggregates still run inside DuckDB, not as a fallback\n")
native <- tryCatch(
  {
    duckplyr::as_duckdb_tibble(fixture, prudence = "stingy") |>
      arrange(g, desc(is.na(d1)), d1, rank2, id3) |>
      mutate(row_order = row_number()) |>
      summarise(
        a = first(x, order_by = row_order),
        b = last(x, order_by = row_order),
        c = nth(x, 2, order_by = row_order),
        .by = g
      ) |>
      as.data.frame()
    TRUE
  },
  error = function(e) FALSE
)
check("row_number() + order_by= is executed by DuckDB", native)

cat("\n[3/3] every collapse in the pipeline passes an explicit order\n")
# A bare first()/last()/nth() inside a summarise() is the bug this guards
# against, so fail if one reappears in a pipeline script.
scripts <- c(
  "scripts/eoir_proceeding.R", "scripts/eoir_appeals.R",
  "scripts/eoir_associated_bond.R", "scripts/eoir_court_applications.R",
  "scripts/eoir_custody_history.R", "scripts/eoir_proceedings_charges.R"
)
for (path in scripts) {
  src <- readLines(path, warn = FALSE)
  # calls to first()/last()/nth() that assign a summarised column
  agg_lines <- grep("=\\s*(dplyr::)?(first|last|nth)\\(", src, value = TRUE)
  # ignore commented-out lines
  agg_lines <- agg_lines[!grepl("^\\s*#", agg_lines)]
  missing <- agg_lines[!grepl("order_by\\s*=\\s*row_order", agg_lines)]
  check(
    sprintf("%s (%d aggregates, all ordered)", basename(path), length(agg_lines)),
    length(agg_lines) > 0 && length(missing) == 0
  )
  if (length(missing)) {
    for (l in utils::head(missing, 3)) cat("         unordered:", trimws(l), "\n")
  }
}

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d aggregate-ordering check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All aggregate-ordering checks passed.\n")
