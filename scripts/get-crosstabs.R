library(tidyverse)
library(tidylog)

msgs <- tbls <- list()
fls <- list.files("inputs", pattern = "\\.csv$", full.names = TRUE)
for (f in fls) {
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

  # msgs[[f]] <- summarize(
  #   df,
  #   across(
  #     everything(),
  #     ~ mean(is.na(.x))
  #   )
  # )

  # calculate table of value frequencies
  tbls[[f]] <-
    df |>
    select(where(~ n_distinct(.) < 25)) |>
    imap_dfr(~ as.data.frame(table(.x)) |> mutate(name = .y))

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
  write_csv("data/missingness_by_field.csv")

tbls |>
  bind_rows() |>
  mutate(
    value = if_else(value %in% na_vals, NA_character_, value)
  ) |>
  group_by(NAME) |>
  mutate(
    pct = n / sum(n),
    cum_pct = cumsum(pct)
  ) |>
  ungroup() |>
  arrange(NAME, desc(n), value) |>
  write_csv("data/value_frequencies_by_field.csv")
