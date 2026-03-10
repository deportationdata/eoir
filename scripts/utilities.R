# Shared utilities for EOIR CSV cleaning
# Handles: control characters, extra tabs (mid-row shifts & end-of-row overflow)

library(data.table)
library(stringr)
library(dplyr)
library(readr)

#' Read an EOIR TSV file with standardized parameters and row-count validation.
#' Returns a data.table.
read_eoir_tsv <- function(file, col_types = "character") {
  # fread with quote="" stops early on rows with MORE fields than the header,
  # even with fill=TRUE. Fix: pad the header line with extra tab-separated
  # overflow column names so fread sees a consistent field count.
  n_header <- as.integer(system(
    sprintf("awk -F'\t' 'NR==1{print NF; exit}' %s", shQuote(file)),
    intern = TRUE
  ))
  max_fields <- as.integer(system(
    sprintf(
      "awk -F'\t' 'BEGIN{m=0} NF>m{m=NF} END{print m}' %s",
      shQuote(file)
    ),
    intern = TRUE
  ))
  if (max_fields > n_header) {
    # Build a padded version of the header and prepend it to the data
    header_line <- readLines(file, n = 1L, warn = FALSE)
    extra_names <- paste0("V", seq_len(max_fields - n_header))
    padded_header <- paste0(
      header_line,
      "\t",
      paste(extra_names, collapse = "\t")
    )
    tmp <- tempfile(fileext = ".tsv")
    on.exit(unlink(tmp), add = TRUE)
    # Write padded header + rest of file (skip original header)
    system(sprintf(
      "{ echo %s; tail -n +2 %s; } > %s",
      shQuote(padded_header),
      shQuote(file),
      shQuote(tmp)
    ))
    # Read as all-character with no na.strings so overflow columns preserve
    # values like "N/A" that auto_fix_tab_shifts needs to detect shifted rows.
    # Callers convert NAs after shift-fixing via type_convert(na = ...).
    dt <- fread(
      tmp,
      sep = "\t",
      quote = "",
      header = TRUE,
      na.strings = "",
      colClasses = "character",
      fill = TRUE,
      showProgress = FALSE
    )
  } else {
    dt <- fread(
      file,
      sep = "\t",
      quote = "",
      header = TRUE,
      na.strings = c("", "NA", "N/A", "NULL"),
      colClasses = col_types,
      fill = TRUE,
      showProgress = FALSE
    )
  }
  tbl_name <- tools::file_path_sans_ext(basename(file))
  count_file <- file.path(dirname(file), "Count.txt")
  if (file.exists(count_file)) {
    count_lines <- read_lines(count_file)
    expected <- count_lines |>
      keep(~ str_detect(., paste0("^", tbl_name, "\\t"))) |>
      str_extract("\\d+") |>
      as.integer()
    if (length(expected) == 1L) {
      diff <- abs(nrow(dt) - expected)
      if (diff >= 5) {
        warning(sprintf(
          "read_eoir_tsv: %s row count mismatch (got %d, Count.txt says %d, diff=%d)",
          tbl_name, nrow(dt), expected, diff
        ))
      }
    }
  }
  dt
}

#' Read an EOIR lookup table with standardized parameters.
read_eoir_lookup <- function(file) {
  read_delim(
    file,
    delim = "\t",
    col_types = cols(.default = col_character()),
    na = c("", "NA", "NULL"),
    show_col_types = FALSE
  ) |>
    janitor::clean_names()
}

#' Remove control characters, normalise whitespace, drop overflow columns,
#' and convert NA-like strings to real NAs (needed because overflow-containing
#' files are read with na.strings="" to preserve overflow detection).
clean_eoir_cols <- function(df) {
  na_vals <- c("NA", "N/A", "NULL")
  df |>
    mutate(across(
      where(is.character),
      ~ str_remove_all(.x, "\\p{Cntrl}") |> str_squish()
    )) |>
    select(-matches("^V\\d+$")) |>
    mutate(across(
      where(is.character),
      ~ if_else(.x %in% na_vals | .x == "", NA_character_, .x)
    ))
}

#' Build a shift finder that checks date cols for non-dates and non-date cols
#' for dates. Works for any EOIR table with known date/non-date column sets.
make_shift_finder <- function(date_cols, non_date_cols) {
  date_pat <- "^\\d{4}-\\d{2}-\\d{2}"
  function(row_dt, n_extra) {
    col_names <- colnames(row_dt)
    violations <- integer(0)
    for (col in date_cols) {
      if (!col %in% col_names) {
        next
      }
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && nchar(val) > 0 && !grepl(date_pat, val)) {
        violations <- c(violations, which(col_names == col))
      }
    }
    for (col in non_date_cols) {
      if (!col %in% col_names) {
        next
      }
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && grepl(date_pat, val)) {
        violations <- c(violations, which(col_names == col))
      }
    }
    if (!length(violations)) {
      return(NA_character_)
    }
    shift_idx <- min(violations) - n_extra
    if (shift_idx >= 1) col_names[shift_idx] else NA_character_
  }
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
  # Stop before the row-key column `n` to avoid corrupting the key
  last_col <- ncol(dt)
  if ("n" %in% colnames(dt)) {
    last_col <- which(colnames(dt) == "n") - 1L
  }
  for (j in seq(c_idx, last_col)) {
    j_offset <- j + n_offset
    set(dt, i, j, if (j_offset <= last_col) dt[i, ..j_offset] else "")
  }
  invisible(dt)
}

#' Automatically detect and fix mid-row tab shifts in an fread result.
#'
#' @param dt data.table as read by fread (with fill = TRUE)
#' @param shift_col_finder function(row_dt, n_extra) -> column name or
#'   "CONCAT_THEN_<col>" signal, or NA_character_ if unfixable
#' @param pre_fix optional function(dt, row_n, n_extra) called before shifting
#'   when shift_col_finder returns a "CONCAT_THEN_" prefixed result
#' @return dt with mid-row shifts fixed and unfixable rows removed
auto_fix_tab_shifts <- function(dt, shift_col_finder, pre_fix = NULL) {
  has_content <- function(x) !is.na(x) & nchar(trimws(as.character(x))) > 0

  # Add row key
  dt[, n := .I]
  setkey(dt, n)

  overflow_cols <- grep("^V\\d+$", colnames(dt), value = TRUE)

  # --- Pass 1: fix rows with overflow columns (n_extra known from overflow) ---
  shifted_rows <- integer(0)
  if (length(overflow_cols) > 0L) {
    shifted_mask <- rep(FALSE, nrow(dt))
    for (vc in overflow_cols) {
      shifted_mask <- shifted_mask | has_content(dt[[vc]])
    }
    shifted_rows <- dt[shifted_mask, n]
  }

  if (length(shifted_rows) > 0L) {
    message(sprintf(
      "auto_fix_tab_shifts: %d overflow-shifted rows detected",
      length(shifted_rows)
    ))
  }

  rows_to_drop <- integer(0)

  for (rn in shifted_rows) {
    row_dt <- dt[.(rn)]
    n_extra <- 0L
    for (vc in overflow_cols) {
      if (has_content(row_dt[[vc]])) n_extra <- n_extra + 1L
    }
    shift_col <- shift_col_finder(row_dt, n_extra)
    if (is.na(shift_col)) {
      warning(sprintf(
        "auto_fix_tab_shifts: cannot fix row n=%d (n_extra=%d), dropping",
        rn,
        n_extra
      ))
      rows_to_drop <- c(rows_to_drop, rn)
      next
    }
    if (grepl("^CONCAT_THEN_", shift_col)) {
      actual_col <- sub("^CONCAT_THEN_", "", shift_col)
      if (!is.null(pre_fix)) {
        pre_fix(dt, rn, n_extra)
      }
      shift_col <- actual_col
    }
    shift_left_dt(dt, rn, shift_col, n_extra)
  }

  # --- Pass 2: fix hidden shifts (no overflow, but type violations in-row) ---
  # These occur when the shift pushes data right but the rightmost columns
  # were already empty, so nothing spills into overflow.
  # Detect via the shift_col_finder's non_date_cols: vectorised grep for date
  # patterns in columns that should never contain dates.
  already_fixed <- c(shifted_rows, rows_to_drop)
  # Extract the non_date_cols from shift_col_finder's closure (if built by make_shift_finder)
  finder_env <- environment(shift_col_finder)
  if (exists("non_date_cols", envir = finder_env)) {
    ndc <- get("non_date_cols", envir = finder_env)
    ndc <- intersect(ndc, colnames(dt))
    if (length(ndc) > 0L) {
      # Fast vectorised screen: any non-date column contains a date pattern?
      date_rx <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}"
      suspect_mask <- rep(FALSE, nrow(dt))
      for (col in ndc) {
        suspect_mask <- suspect_mask | grepl(date_rx, as.character(dt[[col]]))
      }
      # Exclude already-fixed rows
      suspect_mask[dt$n %in% already_fixed] <- FALSE
      suspect_rows <- dt[suspect_mask, n]

      if (length(suspect_rows) > 0L) {
        for (n_try in 1:3) {
          hidden_rows <- integer(0)
          for (rn in suspect_rows) {
            shift_col <- shift_col_finder(dt[.(rn)], n_try)
            if (!is.na(shift_col)) hidden_rows <- c(hidden_rows, rn)
          }
          if (length(hidden_rows) == 0L) {
            next
          }
          message(sprintf(
            "auto_fix_tab_shifts: %d hidden-shifted rows detected (n_extra=%d)",
            length(hidden_rows),
            n_try
          ))
          for (rn in hidden_rows) {
            row_dt <- dt[.(rn)]
            shift_col <- shift_col_finder(row_dt, n_try)
            if (grepl("^CONCAT_THEN_", shift_col)) {
              actual_col <- sub("^CONCAT_THEN_", "", shift_col)
              if (!is.null(pre_fix)) {
                pre_fix(dt, rn, n_try)
              }
              shift_col <- actual_col
            }
            shift_left_dt(dt, rn, shift_col, n_try)
          }
          suspect_rows <- setdiff(suspect_rows, hidden_rows)
          already_fixed <- c(already_fixed, hidden_rows)
        }
      }
    }
  }

  if (length(rows_to_drop) > 0L) {
    message(sprintf(
      "auto_fix_tab_shifts: dropped %d unfixable rows",
      length(rows_to_drop)
    ))
    dt <- dt[!n %in% rows_to_drop]
  }

  dt[, n := NULL]
  dt
}
