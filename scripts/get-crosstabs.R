library(tidyverse)
library(tidylog)

msgs <- list()
fls <- list.files("inputs", pattern = "\\.csv$", full.names = TRUE)
for (f in fls[7:length(fls)]) {
  message("Reading ", f)
  df <- data.table::fread(
    f,
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = c("", "NA", "N/A", "NULL"),
    colClasses = "character",
    fill = 55,
    showProgress = FALSE
  )

  df <-
    df |>
    select(-matches("^V\\d+$"))

  na_vals <- c("", "NA", "N/A", "NULL")

  msgs[[f]] <- summarize(
    df,
    across(
      everything(),
      ~ mean(is.na(.x))
    )
  )

  rm(df)
  gc()
}

msgs |>
  map_dfr(
    ~ tibble(name = names(.x), missing_pct = unlist(.x)),
    .id = "table"
  ) |>
  mutate(
    table = str_remove(basename(table), "\\.csv$")
  ) |>
  write_csv("data/missingness_by_table.csv")
