library(tidyverse)
library(tidylog)

cases <- arrow::read_parquet("data/cases.parquet")

cases <-
  cases |>
  mutate(across(
    where(is.character),
    \(x) {
      vals <- sort(unique(na.omit(x)))
      haven::labelled(match(x, vals), labels = setNames(seq_along(vals), vals))
    }
  ))

cases <- if (any(nchar(names(cases)) > 32)) {
  cases |>
    rename_with(
      ~ make.unique(abbreviate(.x, minlength = 32, strict = FALSE), sep = "_")
    )
} else {
  cases
}

haven::write_dta(cases, "data/cases.dta", version = 15)

# use cases.dta
# compress
# save cases.dta, replace

cases |>
  filter(
    !is.na(pretermit_motion_completion_date_last),
    pretermit_motion_decision_code_last == "G",
    case_outcome == "Remove"
  ) |>
  count(bia_decision)
