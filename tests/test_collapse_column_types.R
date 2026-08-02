# Every column that feeds a grouped collapse must be a plain atomic vector.
#
# A column carrying an extra class — the case that bit us was glue::glue(),
# which returns `glue/character` — cannot be aggregated as a plain character
# column, so the grouped first()/last()/nth() falls back to per-group vctrs
# dispatch. Measured on 250k rows / 61k groups: 187s with the glue class
# versus 0.02s without, for byte-identical output. At the real 11.9M rows of
# B_TblProceedCharges that was enough to run eoir_proceedings_charges.R past
# the job's six-hour limit with no error and no output.
#
# The cost is invisible at small scale and the values look correct, so this
# pins the property directly rather than trusting a timing check.
#
# Run: Rscript tests/test_collapse_column_types.R

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

cat("\n[1/3] paste0() and glue() agree on values, but not on class\n")
section <- c("212", "237", NA)
remainder <- c("(a)(6)(C)(i)", "(a)(2)(B)(iv)", "(a)(1)")
glued <- glue::glue("{section}{remainder}")
pasted <- paste0(section, remainder)
check("same characters", identical(as.character(glued), pasted))
check("glue() adds a class", !identical(class(glued), "character"))
check("paste0() does not", identical(class(pasted), "character"))

city <- c("Los Angeles", NA)
code <- c("LOS", "XXX")
check(
  "court_desc: paste0 matches glue, including the NA city",
  identical(as.character(glue::glue("{city} ({code})")), paste0(city, " (", code, ")"))
)

cat("\n[2/3] a classed column really does change the collapse result path\n")
set.seed(5)
n <- 20000
frame <- data.frame(
  g = sample(3000, n, replace = TRUE),
  v = sprintf("212(a)(%d)", sample(9, n, replace = TRUE)),
  stringsAsFactors = FALSE
) |>
  arrange(g) |>
  mutate(row_order = row_number())

collapse <- function(df) {
  df |>
    summarise(
      a = nth(v, 1, order_by = row_order),
      b = nth(v, 2, order_by = row_order),
      .by = g
    ) |>
    arrange(g) |>
    mutate(across(everything(), as.character)) |>
    as.data.frame()
}
plain <- collapse(frame)
classed <- collapse(mutate(frame, v = glue::glue("{v}")))
check("values match either way (the cost is time, not correctness)", isTRUE(all.equal(plain, classed)))
check(
  "plain character collapse is executed by DuckDB",
  tryCatch(
    {
      duckplyr::as_duckdb_tibble(frame, prudence = "stingy") |>
        summarise(a = nth(v, 1, order_by = row_order), .by = g) |>
        as.data.frame()
      TRUE
    },
    error = function(e) FALSE
  )
)

cat("\n[3/3] no pipeline script builds a collapse input with glue()\n")
# glue() anywhere in a table script is the smell; the lookup-table case in
# eoir_case_joins.R was joined straight into `cases` and carried the class
# into the released file.
scripts <- Sys.glob("scripts/eoir_*.R")
for (path in scripts) {
  src <- readLines(path, warn = FALSE)
  hits <- grep("glue::glue|glue\\(", src, value = TRUE)
  hits <- hits[!grepl("^\\s*#", hits)]
  check(sprintf("%s free of glue()", basename(path)), length(hits) == 0)
  for (l in utils::head(hits, 2)) cat("         ", trimws(l), "\n")
}

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d collapse-column-type check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All collapse-column-type checks passed.\n")
