suppressPackageStartupMessages({
  library(arrow)
  library(skimr)
  library(dplyr)
})

new <- arrow::read_parquet("data/cases.parquet")
old <- arrow::read_parquet("main_ref/data/cases.parquet")

new_skim <- as.data.frame(skim(new))
old_skim <- as.data.frame(skim(old))

joined <- inner_join(
  new_skim, old_skim,
  by = c("skim_type", "skim_variable"),
  suffix = c("_new", "_old")
)

flags <- character()

mr <- joined |>
  mutate(shift = (1 - complete_rate_new) - (1 - complete_rate_old)) |>
  filter(!is.na(shift), abs(shift) >= 0.02)
for (i in seq_len(nrow(mr))) {
  flags <- c(flags, sprintf(
    "`%s`: missing rate %+.1fpp (was %.1f%%, now %.1f%%)",
    mr$skim_variable[i], 100 * mr$shift[i],
    100 * (1 - mr$complete_rate_old[i]),
    100 * (1 - mr$complete_rate_new[i])
  ))
}

if (all(c("numeric.mean_new", "numeric.mean_old") %in% names(joined))) {
  nm <- joined |> filter(skim_type == "numeric")
  rel <- abs((nm$numeric.mean_new - nm$numeric.mean_old) / nm$numeric.mean_old)
  for (i in which(is.finite(rel) & rel >= 0.05)) {
    flags <- c(flags, sprintf(
      "`%s`: mean shifted %.1f%% (was %g, now %g)",
      nm$skim_variable[i], 100 * rel[i],
      nm$numeric.mean_old[i], nm$numeric.mean_new[i]
    ))
  }
}

if (all(c("Date.max_new", "Date.max_old") %in% names(joined))) {
  dt <- joined |>
    filter(skim_type == "Date",
           !is.na(Date.max_new), !is.na(Date.max_old),
           Date.max_new < Date.max_old)
  for (i in seq_len(nrow(dt))) {
    flags <- c(flags, sprintf(
      "`%s`: max date regressed (was %s, now %s)",
      dt$skim_variable[i], dt$Date.max_old[i], dt$Date.max_new[i]
    ))
  }
}

added   <- setdiff(names(new), names(old))
removed <- setdiff(names(old), names(new))

out <- c(
  "",
  "### Distribution comparison (skimr)",
  sprintf("Compared %d common columns. (new=%d cols, old=%d cols)",
          nrow(joined), ncol(new), ncol(old))
)
if (length(added))   out <- c(out, sprintf("- Added: %s", paste(sprintf("`%s`", added), collapse = ", ")))
if (length(removed)) out <- c(out, sprintf("- Removed: %s", paste(sprintf("`%s`", removed), collapse = ", ")))
out <- c(out, "")

if (length(flags) == 0) {
  out <- c(out, "All distributions broadly similar; no flags.")
} else {
  out <- c(out, sprintf("**%d flag(s):**", length(flags)),
           paste("-", flags))
}

out <- c(out, "",
         "<details><summary>Full skim of new release</summary>",
         "",
         "```")
out <- c(out, capture.output(print(skim(new))))
out <- c(out, "```", "</details>", "")

writeLines(out, "compare_report.md")
cat(out, sep = "\n")
