# Shared utilities for EOIR CSV cleaning
# Handles: control characters, extra tabs (mid-row shifts & end-of-row overflow)

library(data.table)
library(stringr)
library(dplyr)

#' Remove control characters and normalise whitespace on all character columns
clean_string_cols <- function(df) {
  mutate(df, across(where(is.character),
    ~ str_remove_all(.x, "\\p{Cntrl}") |> str_squish()
  ))
}

#' Drop V1, V2, … overflow columns created by fread when rows have extra tabs
drop_overflow_cols <- function(df) {

  select(df, -matches("^V\\d+$"))
}

#' Shift columns left in a data.table row to fix mid-row tab insertions.
#' Expects dt to have a column `n` used as key.
#' @param dt data.table with setkey(dt, n)
#' @param row_n integer row key (value of `n` column)
#' @param col_name character name of the first column to shift into
#' @param n_offset integer number of positions to shift left
shift_left_dt <- function(dt, row_n, col_name, n_offset) {
  c_idx <- which(colnames(dt) == col_name)
  i <- dt[.(row_n), which = TRUE]
  for (j in seq(c_idx, ncol(dt))) {
    j_offset <- j + n_offset
    set(dt, i, j, if (j_offset <= ncol(dt)) dt[i, ..j_offset] else "")
  }
  invisible(dt)
}

#' Automatically detect and fix mid-row tab shifts in an fread result.
#'
#' @param dt data.table as read by fread (with fill = large value)
#' @param shift_col_finder function(row_dt, n_extra) -> column name or
#'   "CONCAT_THEN_<col>" signal, or NA_character_ if unfixable
#' @param pre_fix optional function(dt, row_n, n_extra) called before shifting
#'   when shift_col_finder returns a "CONCAT_THEN_" prefixed result
#' @return dt with mid-row shifts fixed and unfixable rows removed
auto_fix_tab_shifts <- function(dt, shift_col_finder, pre_fix = NULL) {
  # Identify overflow columns

overflow_cols <- grep("^V\\d+$", colnames(dt), value = TRUE)
  if (length(overflow_cols) == 0L) return(dt)

  # Add row key
  dt[, n := .I]
  setkey(dt, n)

  # Detect mid-row shifted rows: any overflow column has non-empty, non-whitespace content
  has_content <- function(x) !is.na(x) & nchar(trimws(as.character(x))) > 0
  shifted_mask <- rep(FALSE, nrow(dt))
  for (vc in overflow_cols) {
    shifted_mask <- shifted_mask | has_content(dt[[vc]])
  }
  shifted_rows <- dt[shifted_mask, n]

  if (length(shifted_rows) == 0L) {
    dt[, n := NULL]
    return(dt)
  }

  message(sprintf("auto_fix_tab_shifts: %d mid-row shifted rows detected", length(shifted_rows)))

  rows_to_drop <- integer(0)

  for (rn in shifted_rows) {
    # Count extra tabs = number of non-empty overflow columns
    row_dt <- dt[.(rn)]
    n_extra <- 0L
    for (vc in overflow_cols) {
      if (has_content(row_dt[[vc]])) n_extra <- n_extra + 1L
    }

    shift_col <- shift_col_finder(row_dt, n_extra)

    if (is.na(shift_col)) {
      warning(sprintf("auto_fix_tab_shifts: cannot fix row n=%d (n_extra=%d), dropping", rn, n_extra))
      rows_to_drop <- c(rows_to_drop, rn)
      next
    }

    # Handle pre_fix signal (e.g. "CONCAT_THEN_ALIEN_STATE")
    if (grepl("^CONCAT_THEN_", shift_col)) {
      actual_col <- sub("^CONCAT_THEN_", "", shift_col)
      if (!is.null(pre_fix)) {
        pre_fix(dt, rn, n_extra)
      }
      shift_col <- actual_col
    }

    shift_left_dt(dt, rn, shift_col, n_extra)
  }

  if (length(rows_to_drop) > 0L) {
    message(sprintf("auto_fix_tab_shifts: dropped %d unfixable rows", length(rows_to_drop)))
    dt <- dt[!n %in% rows_to_drop]
  }

  dt[, n := NULL]
  dt
}
