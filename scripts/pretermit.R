cases |>
  filter(
    !is.na(pretermit_motion_completion_date_last),
    pretermit_motion_decision_code_last == "G",
    case_outcome == "Remove"
  ) |>
  filter(!is.na(appeal_filed_date), !is.na(bia_decision_date)) |>
  count(bia_decision)
