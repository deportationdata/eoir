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

  msgs[[f]] <- summarize(
    df,
    across(
      everything(),
      ~ mean(is.na(.x))
    )
  )

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
  bind_rows(.id = "table") |>
  mutate(
    table = str_remove(basename(table), "\\.csv$")
  ) |>
  as_tibble() |>
  rename(value = .x, n = Freq) |>
  mutate(
    value = if_else(value %in% na_vals, NA_character_, value)
  ) |>
  group_by(name) |>
  mutate(
    pct = n / sum(n)
  ) |>
  ungroup() |>
  arrange(name, desc(n), value) |>
  write_csv("data/value_frequencies_by_field.csv")

lookup_tbls <- list.files("inputs/Lookup", recursive = TRUE, full.names = TRUE)

# ---- helpers ----------------------------------------------------------

# first non-NA element or NA_character_
first_or_na <- function(x) {
  if (length(x) == 0) {
    return(NA_character_)
  }
  x[1]
}

# Pick the first column name matching any regex in `patterns`,
# while excluding anything that matches `exclude_patterns`.
pick_col <- function(nms, patterns, exclude_patterns = character()) {
  keep <- map_lgl(
    nms,
    ~ any(str_detect(.x, patterns)) && !any(str_detect(.x, exclude_patterns))
  )
  if (!any(keep)) {
    return(NA_character_)
  }
  nms[which(keep)[1]]
}

# Best-effort guess of the variable "name" this lookup is for
guess_varname <- function(df_names, file_path, code_col = NA_character_) {
  # 1) from an id-like column (idn*, id*, ...), avoid date-ish
  id_like <- pick_col(
    df_names,
    patterns = c("^idn", "^id(_|$)"),
    exclude_patterns = c("date", "^dat", "mod")
  )
  if (!is.na(id_like)) {
    v <- id_like |>
      str_remove("^idn") |>
      str_remove("^id") |>
      str_remove("_?(id|key|pk)$") |>
      str_replace_all("[^a-z0-9]+", "_") |>
      str_remove("^_+") |>
      str_remove("_+$")
    if (nzchar(v)) return(v)
  }

  # 2) from the filename
  base <- basename(file_path) |> tools::file_path_sans_ext() |> tolower()
  from_file <- base |>
    str_remove("^tbl") |>
    str_remove("^tbllookup") |>
    str_remove("^lookup") |>
    str_replace_all("[^a-z0-9]+", "_") |>
    str_remove("^_+") |>
    str_remove("_+$")
  if (nzchar(from_file)) {
    return(from_file)
  }

  # 3) from the code column stem
  if (!is.na(code_col)) {
    stem <- code_col |>
      str_remove("_?code$") |>
      str_remove("^str") |>
      str_replace_all("[^a-z0-9]+", "_") |>
      str_remove("^_+") |>
      str_remove("_+$")
    if (nzchar(stem)) return(stem)
  }

  "unknown"
}

# ---- main -------------------------------------------------------------

lookup_tbl <-
  lookup_tbls |>
  set_names() |>
  map_dfr(
    ~ {
      # read as character; clean names to snake_case lower
      df <- readr::read_delim(
        .x,
        col_types = cols(.default = "c")
      ) |>
        janitor::clean_names()

      nms <- names(df)

      # CODE column candidates
      code_col <- pick_col(
        nms,
        patterns = c(
          "^strcode$", # canonical
          "_code$", # state_code, nat_code, notice_code, etc.
          "code$", # loose fallback
          "^notice_code$",
          "^nat_code$",
          "^languagecode$",
          "^intrcode$",
          "^insloc_code$",
          "^hearing_loc_code$",
          "^strcompcode$",
          "^strmotioncode$",
          "^strdec(code)?$" # strDecCode, strDec
        ),
        exclude_patterns = c("zip_code", "zipcode") # avoid zip
      )

      # DESCRIPTION-like column candidates
      desc_col <- pick_col(
        nms,
        patterns = c(
          "^strdescription$", # canonical
          "description$",
          "decsription$", # misspelling too
          "desc$",
          "_name$", # judge_name, nat_name, hearing_loc_name
          "_disp$", # notice_disp
          "code?description$" # strCodeDescription
        )
      )

      # bail if we don't have a plausible pair
      if (is.na(code_col) || is.na(desc_col)) {
        return(tibble::tibble(
          table = basename(.x),
          value = character(),
          description = character(),
          name = character()
        ))
      }

      varname <- guess_varname(nms, .x, code_col)

      df |>
        transmute(
          value = .data[[code_col]],
          description = .data[[desc_col]],
          name = varname
        )
    },
    .id = "table"
  )

write_csv(lookup_tbl, "data/lookup_tables_combined.csv")
