suppressPackageStartupMessages({
  library(arrow); library(skimr); library(dplyr)
})

# Usage: Rscript scripts/compare_distributions.R [new.parquet] [old.parquet] [out.md]
args     <- commandArgs(trailingOnly = TRUE)
new_path <- if (length(args) >= 1) args[1] else "data/cases.parquet"
old_path <- if (length(args) >= 2) args[2] else "main_ref/data/cases.parquet"
out_path <- if (length(args) >= 3) args[3] else "compare_report.md"

# Value counts for every character column (small: one row per distinct value),
# used below to list values that appear in only one release.
value_counts <- function(df) {
  cols <- names(df)[vapply(df, is.character, logical(1))]
  setNames(lapply(cols, function(col) {
    vc <- vctrs::vec_count(df[[col]], sort = "count")
    vc[!is.na(vc$key), ]
  }), cols)
}

# Skim each side independently and bind the (small) result tables. Materializing
# the union of both parquet files via bind_rows() previously OOM-killed the
# runner on large updates.
old <- arrow::read_parquet(old_path)
old_cols <- names(old)
old_skim <- as.data.frame(skim(old))
old_vals <- value_counts(old)
rm(old); invisible(gc())

new <- arrow::read_parquet(new_path)
new_cols <- names(new)
new_skim <- as.data.frame(skim(new))
new_vals <- value_counts(new)
rm(new); invisible(gc())

joined <- inner_join(
  new_skim, old_skim,
  by = c("skim_type", "skim_variable"),
  suffix = c("_new", "_old")
)

# ---- formatting helpers -----------------------------------------------------

fmt_int <- function(x) ifelse(is.na(x), "", format(x, big.mark = ",", scientific = FALSE, trim = TRUE))
fmt_signed_int <- function(x) ifelse(is.na(x) | x == 0, "", sprintf("%s%s", ifelse(x > 0, "+", "−"), fmt_int(abs(x))))
fmt_num <- function(x) vapply(x, function(v) {
  if (is.na(v)) return("")
  v <- if (abs(v) >= 1e5) round(v) else signif(v, 4)
  format(v, big.mark = ",", scientific = FALSE, trim = TRUE)
}, "")
fmt_rate <- function(x) ifelse(is.na(x), "", formatC(x, digits = 3, format = "f"))
fmt_cell <- function(x) {
  if (is.numeric(x)) fmt_num(x) else ifelse(is.na(x), "", gsub("\\|", "\\\\|", as.character(x)))
}

# Per-variable change summary shown on the "new" row. Pieces that cross the
# flag thresholds are wrapped in ** and also reported in the summary bullets.
MISSING_PP_FLAG <- 2
MEAN_PCT_FLAG   <- 5

describe_change <- function(r) {
  pieces <- character(); flags <- character()
  push <- function(txt, flagged = FALSE) {
    pieces <<- c(pieces, if (flagged) paste0("**", txt, "**") else txt)
    if (flagged) flags <<- c(flags, txt)
  }
  d_miss <- r$n_missing_new - r$n_missing_old
  pp <- 100 * ((1 - r$complete_rate_new) - (1 - r$complete_rate_old))
  if (!is.na(d_miss) && d_miss != 0)
    push(sprintf("missing %s%s", fmt_signed_int(d_miss),
         if (abs(pp) >= 0.005) sprintf(" (%+.2fpp)", pp) else ""), abs(pp) >= MISSING_PP_FLAG)
  if (r$skim_type == "numeric" && !is.na(r$numeric.mean_old) && r$numeric.mean_old != 0) {
    pct <- 100 * (r$numeric.mean_new - r$numeric.mean_old) / r$numeric.mean_old
    if (!is.na(pct) && abs(pct) >= 0.05)
      push(sprintf("mean %+.1f%%", pct), abs(pct) >= MEAN_PCT_FLAG)
  }
  if (r$skim_type == "Date" && !is.na(r$Date.max_new) && !is.na(r$Date.max_old) &&
      r$Date.max_new != r$Date.max_old)
    push(sprintf("max %s → %s", r$Date.max_old, r$Date.max_new), r$Date.max_new < r$Date.max_old)
  nu_old <- r[[paste0(r$skim_type, ".n_unique_old")]]
  nu_new <- r[[paste0(r$skim_type, ".n_unique_new")]]
  if (!is.null(nu_old) && !is.na(nu_old) && !is.na(nu_new) && nu_new != nu_old)
    push(sprintf("n_unique %s", fmt_signed_int(nu_new - nu_old)))
  list(text = paste(pieces, collapse = "; "), flag = paste(flags, collapse = "; "))
}

changes <- lapply(seq_len(nrow(joined)), function(i) describe_change(joined[i, ]))
joined$change_text <- vapply(changes, `[[`, "", "text")
joined$flag        <- vapply(changes, `[[`, "", "flag")
flagged <- joined |> filter(nzchar(flag)) |> select(skim_variable, flag)

# ---- stacked old/new table per variable type --------------------------------

# One markdown table per skim type. Each variable gets two consecutive rows
# (old on top, new below) so every stat column reads as an old/new pair.
stacked_table <- function(type) {
  rows <- joined |> filter(skim_type == type) |> arrange(match(skim_variable, new_cols))
  if (nrow(rows) == 0) return(character())
  stats <- c("n_missing", "complete_rate",
             sub(paste0("^", type, "\\."), "", grep(paste0("^", type, "\\..*_new$"), names(rows), value = TRUE) |>
               sub(pattern = "_new$", replacement = "")))
  stats <- unique(stats)
  col_of <- function(stat, side) {
    nm <- if (stat %in% c("n_missing", "complete_rate")) stat else paste0(type, ".", stat)
    rows[[paste0(nm, "_", side)]]
  }
  fmt_stat <- function(stat, side) {
    x <- col_of(stat, side)
    if (stat == "n_missing") fmt_int(x) else if (stat == "complete_rate") fmt_rate(x) else fmt_cell(x)
  }
  old_cells <- sapply(stats, fmt_stat, side = "old")
  new_cells <- sapply(stats, fmt_stat, side = "new")
  if (nrow(rows) == 1) { old_cells <- t(old_cells); new_cells <- t(new_cells) }
  old_lines <- sprintf("| `%s` | old | %s |  |", rows$skim_variable, apply(old_cells, 1, paste, collapse = " | "))
  new_lines <- sprintf("|  | **new** | %s | %s |", apply(new_cells, 1, paste, collapse = " | "), rows$change_text)
  header <- sprintf("| variable | release | %s | Δ |", paste(stats, collapse = " | "))
  align  <- sprintf("|---|---|%s|---|", paste(rep("---:", length(stats)), collapse = "|"))
  c(sprintf("#### %s (%d variables)", type, nrow(rows)), "",
    header, align, as.vector(rbind(old_lines, new_lines)), "")
}

types <- intersect(c("character", "Date", "logical", "numeric"), unique(joined$skim_type))
types <- c(types, setdiff(unique(joined$skim_type), types))

# ---- new / removed values of categorical (character) variables --------------

MAX_VALUES_SHOWN <- 30  # per variable per direction

fmt_value <- function(v) {
  v <- gsub("`", "'", v, fixed = TRUE)
  ifelse(v == "", "*(empty string)*", paste0("`", gsub("|", "\\|", v, fixed = TRUE), "`"))
}

# Values present in `a` but not `b`, with their row count in `a`.
value_diff <- function(a, b) {
  out <- lapply(intersect(names(a), names(b)), function(col) {
    d <- a[[col]][!(a[[col]]$key %in% b[[col]]$key), , drop = FALSE]
    if (nrow(d) == 0) return(NULL)
    data.frame(variable = col, value = d$key, n = d$count, stringsAsFactors = FALSE)
  })
  out <- bind_rows(out)
  if (nrow(out) == 0) data.frame(variable = character(), value = character(), n = integer())
  else arrange(out, match(variable, new_cols), desc(n))
}

values_table <- function(diff, side_label) {
  if (nrow(diff) == 0) return("- None.")
  body <- unlist(lapply(split(diff, factor(diff$variable, levels = unique(diff$variable))), function(d) {
    shown <- head(d, MAX_VALUES_SHOWN)
    lines <- sprintf("| %s | %s | %s |",
                     c(paste0("`", d$variable[1], "`"), rep("", nrow(shown) - 1)),
                     fmt_value(shown$value), fmt_int(shown$n))
    if (nrow(d) > MAX_VALUES_SHOWN)
      lines <- c(lines, sprintf("|  | *… and %d more* | %s |", nrow(d) - MAX_VALUES_SHOWN,
                                fmt_int(sum(d$n[-seq_len(MAX_VALUES_SHOWN)]))))
    lines
  }))
  c(sprintf("| variable | value | rows (%s) |", side_label), "|---|---|---:|", body)
}

new_values     <- value_diff(new_vals, old_vals)
removed_values <- value_diff(old_vals, new_vals)
n_vars_new     <- length(unique(new_values$variable))
n_vars_removed <- length(unique(removed_values$variable))

added   <- setdiff(new_cols, old_cols)
removed <- setdiff(old_cols, new_cols)

lines <- c(
  "",
  "### Distribution comparison (skimr)",
  sprintf("Compared %d columns (new=%d, old=%d). Thresholds: missingness ±%gpp, numeric mean ±%g%%, date max moving backwards.",
          nrow(joined), length(new_cols), length(old_cols), MISSING_PP_FLAG, MEAN_PCT_FLAG),
  if (length(added))   sprintf("- Added: %s",   paste0("`", added,   "`", collapse = ", ")),
  if (length(removed)) sprintf("- Removed: %s", paste0("`", removed, "`", collapse = ", ")),
  "",
  if (nrow(flagged) == 0) "All distributions broadly similar; no flags."
  else c(sprintf("**%d flag(s):**", nrow(flagged)),
         sprintf("- `%s`: %s", flagged$skim_variable, flagged$flag)),
  "",
  "<details><summary>Full skim — old (main) and new (this PR) stacked per variable</summary>", "",
  "Each variable has two rows: the prior release on top, this PR below. The Δ column summarises what moved (bold = crosses a flag threshold).", "",
  unlist(lapply(types, stacked_table)),
  "</details>", "",
  sprintf("<details><summary>Categorical values — %d new value(s) across %d variable(s); %d removed value(s) across %d variable(s)</summary>",
          nrow(new_values), n_vars_new, nrow(removed_values), n_vars_removed), "",
  sprintf("Character variables only. At most %d values shown per variable, ordered by row count.", MAX_VALUES_SHOWN), "",
  "#### New values (in this PR, not in main)", "",
  values_table(new_values, "new"), "",
  "#### Removed values (in main, not in this PR)", "",
  values_table(removed_values, "old"), "",
  "</details>", ""
)

writeLines(lines, out_path)
cat(lines, sep = "\n")
