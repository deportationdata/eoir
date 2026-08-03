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

cat("\n[1/3] coalesce() is a drop-in for replace_na()\n")
x <- c("DENY", NA, "GRANT", "")
check(
  "identical on a character column, empty string included",
  identical(replace_na(x, "No application"), coalesce(x, "No application"))
)

cat("\n[2/3] a label join is a drop-in for recode_values()\n")
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

cat("\n[3/3] the wide-frame script uses neither\n")
src <- readLines("scripts/eoir_case_joins.R", warn = FALSE)
src <- src[!grepl("^\\s*#", src)]
for (verb in c("replace_na", "recode_values")) {
  hits <- grep(verb, src, value = TRUE)
  check(sprintf("eoir_case_joins.R free of %s()", verb), length(hits) == 0)
  for (l in utils::head(hits, 2)) cat("         ", trimws(l), "\n")
}

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d case-joins check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All case-joins native-verb checks passed.\n")
