library(tidyverse)
library(duckplyr)

cases <- arrow::read_parquet("data/cases.parquet")

missingness <-
  cases |>
  summarise(across(everything(), \(x) mean(is.na(x)))) |>
  pivot_longer(
    everything(),
    names_to = "variable",
    values_to = "prop_missing"
  ) |>
  arrange(desc(prop_missing))

print(missingness, n = Inf)

write_rds(
  missingness,
  "~/github/deportationdata.org/eoir_missingness_by_field.rds"
)
