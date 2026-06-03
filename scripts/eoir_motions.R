library(tidyverse)
library(tidylog)
library(data.table)
library(pointblank)

source("scripts/utilities.R")

motions_raw <- read_eoir_tsv("inputs_eoir/tbl_Court_Motions.csv")

motions_tbl <-
  motions_raw |>
  select(IDNMOTION, IDNPROCEEDING, IDNCASE, REC_TYPE, MOTION_RECD_DATE) |>
  clean_eoir_cols()

lkp_motion_type <- read_eoir_lookup("inputs_eoir/tblLookupMotionType.csv")

motions_tbl |>
  col_vals_not_null(
    IDNMOTION,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_not_null(
    IDNCASE,
    actions = action_levels(warn_at = 0.005, stop_at = 0.01)
  ) |>
  col_vals_regex(IDNMOTION, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNPROCEEDING, "^\\d+$", na_pass = TRUE) |>
  col_vals_regex(IDNCASE, "^\\d+$", na_pass = TRUE) |>
  col_vals_in_set(
    REC_TYPE,
    c(lkp_motion_type$str_motion_code, NA),
    actions = action_levels(warn_at = 0.0001, stop_at = 0.005)
  ) |>
  col_vals_regex(
    MOTION_RECD_DATE,
    "^\\d{4}-\\d{2}-\\d{2}",
    na_pass = TRUE,
    actions = action_levels(warn_at = 0.0001, stop_at = 0.001)
  ) |>
  invisible()

motions_tbl <- fast_convert(
  motions_tbl,
  list(
    IDNMOTION = "integer",
    IDNPROCEEDING = "integer",
    IDNCASE = "integer",
    MOTION_RECD_DATE = "date"
  )
)

motions_tbl <-
  motions_tbl |>
  janitor::clean_names() |>
  mutate(motion_to_pretermit = rec_type == "PM")

# setDT(motions_tbl)

motions_by_case <-
  motions_tbl |>
  filter(motion_to_pretermit) |>
  distinct(
    idncase,
    motion_to_pretermit,
    motion_pretermit_date = motion_recd_date
  )

# motions_by_case <-
#   motions_tbl[,
#     .(
#       motion_to_pretermit = any(rec_type == "PM")
#     ),
#     by = idncase
#   ] |>
#   filter(!is.na(idncase))

motions_by_case |>
  rows_distinct(idncase) |>
  invisible()

arrow::write_parquet(
  motions_by_case,
  "tmp/motions_cases.parquet"
)
