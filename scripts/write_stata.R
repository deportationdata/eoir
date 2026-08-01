library(tidyverse)
library(duckplyr)

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

haven::write_dta(cases, "data/cases.dta", version = 15)

# use cases.dta
# compress
# save cases.dta, replace
