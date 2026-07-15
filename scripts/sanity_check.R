suppressPackageStartupMessages({
  library(arrow)
})

args <- commandArgs(trailingOnly = TRUE)
new_path <- if (length(args) >= 1) args[1] else "data/cases.parquet"
old_path <- if (length(args) >= 2) args[2] else "main_ref/data/cases.parquet"
last_modified_path <- if (length(args) >= 3) args[3] else ".last_modified"
report_path <- if (length(args) >= 4) args[4] else "sanity_report.md"

RECENCY_TOLERANCE_DAYS <- 7L
RECENCY_COL <- "ij_or_bia_completion_date_last"

DEPORTED_OUTCOME_TOLERANCE_PCT <- 0.5
EXPECTED_DEPORTED_OUTCOMES <- c("Remove", "Voluntary Departure")

new <- arrow::read_parquet(new_path)
old <- tryCatch(arrow::read_parquet(old_path), error = function(e) NULL)

fmt_int <- function(x) format(x, big.mark = ",")
fmt_signed <- function(x) sprintf("%s%s", if (x >= 0) "+" else "", fmt_int(x))

warnings_md <- character()
warn <- function(msg) warnings_md[[length(warnings_md) + 1L]] <<- msg

out <- c()
add <- function(...) out[[length(out) + 1L]] <<- paste0(...)

add("## Data sanity check\n")

n_new <- nrow(new)
add("### Row counts\n")
if (!is.null(old)) {
  n_old <- nrow(old)
  delta <- n_new - n_old
  pct <- if (n_old > 0) 100 * delta / n_old else NA_real_
  add(sprintf(
    "| Metric | This PR | Prior (main) | Δ |\n|---|---:|---:|---:|\n| `cases.parquet` rows | %s | %s | %s (%+0.3f%%) |\n",
    fmt_int(n_new),
    fmt_int(n_old),
    fmt_signed(delta),
    pct
  ))
  if (delta < 0) {
    warn(sprintf(
      "Row count decreased by %s (%+0.3f%%).",
      fmt_int(abs(delta)),
      pct
    ))
  } else if (!is.na(pct) && pct < 0.01) {
    warn(sprintf("Row count grew by only %s (%+0.4f%%).", fmt_int(delta), pct))
  }
} else {
  add(sprintf(
    "- `cases.parquet` rows: **%s** (no prior version on main)\n",
    fmt_int(n_new)
  ))
}

add("\n### Data recency\n")

last_mod_raw <- tryCatch(
  readLines(last_modified_path, n = 1L, warn = FALSE),
  error = function(e) NA_character_
)
last_mod <- suppressWarnings(as.Date(last_mod_raw, format = "%a, %d %b %Y"))

if (RECENCY_COL %in% names(new)) {
  vals <- suppressWarnings(as.Date(new[[RECENCY_COL]]))
  vals <- vals[!is.na(vals) & vals <= Sys.Date() + 1L]
  event_max <- if (length(vals)) max(vals) else NA
} else {
  event_max <- NA
}

if (!is.na(last_mod) && !is.na(event_max)) {
  gap <- as.integer(last_mod - event_max)
  add(sprintf(
    "- File `Last-Modified`: **%s**\n- Latest `%s` in data: **%s**\n- Gap: **%d day(s)** (tolerance: %d)\n",
    format(last_mod),
    RECENCY_COL,
    format(event_max),
    gap,
    RECENCY_TOLERANCE_DAYS
  ))
  if (gap > RECENCY_TOLERANCE_DAYS) {
    warn(sprintf(
      "Latest `%s` is %d day(s) before the file's `Last-Modified` header (tolerance %d).",
      RECENCY_COL,
      gap,
      RECENCY_TOLERANCE_DAYS
    ))
  }
} else {
  add(sprintf(
    "- File `Last-Modified`: %s\n- Latest `%s`: %s\n",
    if (is.na(last_mod)) "(unparsed)" else format(last_mod),
    RECENCY_COL,
    if (is.na(event_max)) "(missing)" else format(event_max)
  ))
}

if (!is.null(old)) {
  added <- setdiff(names(new), names(old))
  removed <- setdiff(names(old), names(new))
  if (length(added) || length(removed)) {
    add("\n### Schema changes\n")
    if (length(added)) {
      add(sprintf("- Added: %s\n", paste0("`", added, "`", collapse = ", ")))
    }
    if (length(removed)) {
      add(sprintf(
        "- Removed: %s\n",
        paste0("`", removed, "`", collapse = ", ")
      ))
    }
    warn("Schema changed since prior release.")
  }
}

add("\n### Warnings\n")
if (length(warnings_md) == 0L) {
  add("- None.\n")
} else {
  for (w in warnings_md) {
    add(sprintf("- %s\n", w))
  }
}

writeLines(unlist(out), report_path)
cat(readLines(report_path), sep = "\n")
