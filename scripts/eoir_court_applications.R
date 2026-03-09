library(tidyverse)
library(tidylog)
library(data.table)

source("scripts/utilities.R")

court_applications_tbl <-
  read_eoir_tsv("inputs_eoir/tbl_Court_Appln.csv") |>
  as_tibble() |>
  clean_eoir_cols() |>
  janitor::clean_names() |>
  mutate(idncase = as.integer(idncase))

setDT(court_applications_tbl)

court_applications_by_case <-
  court_applications_tbl[,
    .(
      asylumapp = any(appl_code == "ASYL"),
      withholdapp = any(appl_code == "ASYW"),
      catapp = any(appl_code == "WCAT"),
      adjustapp = any(appl_code == "245"),
      nonlprcancelapp = any(appl_code == "42B"),
      lprcancelapp = any(appl_code == "42A"),
      anyreliefapp = any(!is.na(appl_code) & appl_code != "VD")
    ),
    by = idncase
  ] |>
  as_tibble() |>
  filter(!is.na(idncase))

arrow::write_feather(
  court_applications_by_case,
  "tmp/court_applications_cases.feather"
)
