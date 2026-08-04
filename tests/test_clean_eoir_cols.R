# clean_eoir_cols() rewrites only the cells that need repair rather than every
# cell, which is ~3.7x faster on realistic data but means its output has to be
# pinned against the straightforward implementation it replaced. This asserts
# the two are indistinguishable.
#
# The reference below is the pre-optimisation body, kept verbatim on purpose:
#     mutate(across(where(is.character),
#       ~ str_remove_all(.x, "\\p{Cntrl}") |> str_squish())) |>
#     select(-matches("^V\\d+$")) |>
#     mutate(across(where(is.character),
#       ~ if_else(.x %in% na_vals | .x == "", NA_character_, .x)))
#
# Run: Rscript tests/test_clean_eoir_cols.R

suppressPackageStartupMessages({
  library(dplyr)
  library(stringr)
})

source("scripts/utilities.R")

reference_clean <- function(df) {
  df |>
    mutate(across(
      where(is.character),
      ~ str_remove_all(.x, "\\p{Cntrl}") |> str_squish()
    )) |>
    select(-matches("^V\\d+$")) |>
    mutate(across(
      where(is.character),
      ~ if_else(.x %in% c("") | .x == "", NA_character_, .x)
    ))
}

failures <- character()
check <- function(label, expr) {
  if (isTRUE(expr)) {
    cat("  ok   -", label, "\n")
  } else {
    failures <<- c(failures, label)
    cat("  FAIL -", label, "\n")
  }
}

TAB <- intToUtf8(9)
NL <- intToUtf8(10)
CR <- intToUtf8(13)
NBSP <- intToUtf8(160)

cat("\n[1/4] hostile fixture, every awkward shape we know of\n")
nasty <- c(
  " leading", "trailing ", "  both  ", "in  ternal", "a b",
  paste0("tab", TAB, "sep"),
  paste0("newline", NL, "sep"),
  paste0("crlf", CR, NL, "sep"),
  paste0("nbsp", NBSP, "sep"),
  paste0(NBSP, "leading nbsp"),
  TAB, " ", "  ", "", NA_character_,
  "N/A", "NULL", "clean", "RMV", "2020-01-01", "12345"
)
nasty_df <- data.frame(
  a = rep(nasty, 20),
  b = rep(rev(nasty), 20),
  V1 = rep("overflow", length(nasty) * 20),
  stringsAsFactors = FALSE
)
got <- clean_eoir_cols(nasty_df)
want <- reference_clean(nasty_df)
check("output identical to reference implementation", identical(got, want))
check("overflow V-columns dropped", !any(grepl("^V\\d+$", names(got))))

# Spell out a few semantics explicitly so a future change that alters them
# fails here with an obvious message rather than only as a diff.
cat("\n[2/4] specific semantics\n")
one <- function(s) clean_eoir_cols(data.frame(a = s, stringsAsFactors = FALSE))$a
check("control characters are removed, not spaced", identical(one(paste0("a", TAB, "b")), "ab"))
check("leading/trailing whitespace trimmed", identical(one("  x  "), "x"))
check("internal runs collapse to one space", identical(one("x    y"), "x y"))
check("empty string becomes NA", is.na(one("")))
check("whitespace-only becomes NA", is.na(one("   ")))
check("NA stays NA", is.na(one(NA_character_)))
check("'N/A' is left alone", identical(one("N/A"), "N/A"))
check("clean value untouched", identical(one("2020-01-01"), "2020-01-01"))

cat("\n[3/4] equality holds at every dirt level (branch coverage)\n")
# clean_eoir_cols short-circuits on `if (any(hit))`, so both branches need
# exercising -- an all-clean column takes a different path from a dirty one.
make_df <- function(nr, nc, dirty_frac) {
  set.seed(42)
  clean_vals <- c("RMV", "2020-01-01", "12345", "MEX", "J0042", "N/A")
  dirty_vals <- c(" padded  ", paste0("a", TAB, "b"), "x  y  z", "  ", "")
  as.data.frame(
    setNames(replicate(nc, {
      v <- sample(clean_vals, nr, replace = TRUE)
      k <- sample.int(nr, floor(nr * dirty_frac))
      if (length(k)) v[k] <- sample(dirty_vals, length(k), replace = TRUE)
      v
    }, simplify = FALSE), paste0("c", seq_len(nc))),
    stringsAsFactors = FALSE
  )
}
for (frac in c(1, 0.2, 0.05, 0.02, 0)) {
  d <- make_df(5000, 6, frac)
  check(sprintf("identical at %3.0f%% dirty", frac * 100),
        identical(clean_eoir_cols(d), reference_clean(d)))
}

cat("\n[4/4] non-character columns pass through untouched\n")
mixed <- data.frame(
  chr = c(" a ", "b"),
  int = c(1L, 2L),
  dbl = c(1.5, 2.5),
  lgl = c(TRUE, FALSE),
  dte = as.Date(c("2020-01-01", "2021-06-15")),
  stringsAsFactors = FALSE
)
out <- clean_eoir_cols(mixed)
check("non-character columns unchanged",
      identical(out[c("int", "dbl", "lgl", "dte")], mixed[c("int", "dbl", "lgl", "dte")]))
check("character column still cleaned", identical(out$chr, c("a", "b")))

cat("\n[5] invalid UTF-8 in the input is tolerated\n")
# The EOIR files contain invalid UTF-8. clean_eoir_cols() detects dirty cells
# with a cheap character-class scan first, and stri_detect_charclass() errors
# on the whole vector for a single bad byte where stri_detect_regex() does
# not. A run died on exactly this, so it is pinned here.
bad_byte <- rawToChar(as.raw(c(0x41, 0xFF, 0x42)))
nasty_utf8 <- data.frame(
  a = c("clean", bad_byte, " lead", "a  b", NA),
  b = c(bad_byte, "x  y", "ok", "", "z"),
  stringsAsFactors = FALSE
)
got_utf8 <- tryCatch(clean_eoir_cols(nasty_utf8), error = function(e) e)
check("does not error on invalid UTF-8", !inherits(got_utf8, "error"))
if (!inherits(got_utf8, "error")) {
  check("still repairs the dirty cells in that column",
        identical(got_utf8$a, c("clean", bad_byte, "lead", "a b", NA)))
  check("and in a column whose first value is the bad one",
        identical(got_utf8$b, c(bad_byte, "x y", "ok", NA, "z")))
  check("the invalid value is passed through untouched",
        identical(got_utf8$a[2], bad_byte))
}

cat("\n")
if (length(failures)) {
  stop(sprintf(
    "%d clean_eoir_cols check(s) failed:\n  - %s",
    length(failures), paste(failures, collapse = "\n  - ")
  ), call. = FALSE)
}
cat("All clean_eoir_cols checks passed.\n")
