suppressPackageStartupMessages({
  library(arrow); library(skimr); library(dplyr)
})

new <- arrow::read_parquet("data/cases.parquet")
old <- arrow::read_parquet("main_ref/data/cases.parquet")

# One grouped skim gives both side-by-side rows for display AND the per-release
# stats we wide-join below for flag detection.
combined_skim <- bind_rows(
  old |> mutate(.release = "old"),
  new |> mutate(.release = "new")
) |> group_by(.release) |> skim()

new_skim <- combined_skim |> filter(.release == "new")
old_skim <- combined_skim |> filter(.release == "old")

joined <- inner_join(
  as.data.frame(new_skim), as.data.frame(old_skim),
  by = c("skim_type", "skim_variable"),
  suffix = c("_new", "_old")
)

flagged <- joined |>
  rowwise() |>
  mutate(flag = paste(c(
    if (!is.na(complete_rate_new) && abs((1 - complete_rate_new) - (1 - complete_rate_old)) >= 0.02)
      sprintf("missing %+.1fpp", 100 * ((1 - complete_rate_new) - (1 - complete_rate_old))),
    if (skim_type == "numeric" && !is.na(numeric.mean_old) && numeric.mean_old != 0 &&
        abs((numeric.mean_new - numeric.mean_old) / numeric.mean_old) >= 0.05)
      sprintf("mean %+.1f%%", 100 * (numeric.mean_new - numeric.mean_old) / numeric.mean_old),
    if (skim_type == "Date" && !is.na(Date.max_new) && !is.na(Date.max_old) && Date.max_new < Date.max_old)
      sprintf("max date %s → %s", Date.max_old, Date.max_new)
  ), collapse = "; ")) |>
  ungroup() |>
  filter(nzchar(flag)) |>
  select(skim_variable, flag)

added   <- setdiff(names(new), names(old))
removed <- setdiff(names(old), names(new))

lines <- c(
  "",
  "### Distribution comparison (skimr)",
  sprintf("Compared %d columns (new=%d, old=%d).", nrow(joined), ncol(new), ncol(old)),
  if (length(added))   sprintf("- Added: %s",   paste0("`", added,   "`", collapse = ", ")),
  if (length(removed)) sprintf("- Removed: %s", paste0("`", removed, "`", collapse = ", ")),
  "",
  if (nrow(flagged) == 0) "All distributions broadly similar; no flags."
  else c(sprintf("**%d flag(s):**", nrow(flagged)),
         sprintf("- `%s`: %s", flagged$skim_variable, flagged$flag)),
  "",
  "<details><summary>Full skim — old vs new, side by side per variable</summary>", "", "```",
  capture.output(print(combined_skim)),
  "```", "</details>", ""
)

writeLines(lines, "compare_report.md")
cat(lines, sep = "\n")
