# Shared utilities for EOIR CSV cleaning
# Handles: control characters, extra tabs (mid-row shifts & end-of-row overflow)

library(data.table)
library(stringr)
library(dplyr)
library(readr)
library(pointblank)

#' Read an EOIR TSV file with standardized parameters and row-count validation.
#' Returns a data.table.
read_eoir_tsv <- function(file, col_types = "character") {
  # fread with quote="" stops early on rows with MORE fields than the header,
  # even with fill=TRUE. Fix: pad the header line with extra tab-separated
  # overflow column names so fread sees a consistent field count.

  # Count tab-separated fields on header line only (NR==1), then exit
  n_header <- as.integer(system(
    sprintf("awk -F'\t' 'NR==1{print NF; exit}' %s", shQuote(file)),
    intern = TRUE
  ))
  # Scan the entire file to find the maximum number of tab-separated fields
  # on any single row (tracks running max `m`, prints it at EOF)
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
    # Write padded header + rest of file (skip original header).
    # Uses shell grouping { echo ...; tail -n +2 ...; } to concatenate:
    #   - echo: the header line with extra dummy column names appended
    #   - tail -n +2: all data rows (skipping original header)
    # The combined output is redirected to a temp file for fread.
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
          tbl_name,
          nrow(dt),
          expected,
          diff
        ))
      }
    }
  }
  dt
}

#' Read an EOIR lookup table with standardized parameters.
read_eoir_lookup <- function(file) {
  # Read as raw bytes and strip embedded null bytes that cause readr warnings
  raw_bytes <- readBin(file, "raw", file.info(file)$size)
  raw_bytes <- raw_bytes[raw_bytes != as.raw(0L)]
  read_delim(
    I(rawToChar(raw_bytes)),
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
    # Strip invisible control chars (e.g. \x00, \r) and collapse repeated
    # whitespace — the raw EOIR CSVs are littered with these
    mutate(across(
      where(is.character),
      ~ str_remove_all(.x, "\\p{Cntrl}") |> str_squish()
    )) |>
    # Drop the dummy overflow columns (V1, V2, ...) that were added in
    # read_eoir_tsv to accommodate rows with extra tab-separated fields
    select(-matches("^V\\d+$")) |>
    # Convert sentinel strings to proper R NAs (deferred from read_eoir_tsv
    # so that auto_fix_tab_shifts can distinguish real data from blanks)
    mutate(across(
      where(is.character),
      ~ if_else(.x %in% na_vals | .x == "", NA_character_, .x)
    ))
}

#' Build a shift finder that detects mid-row column shifts by looking for
#' type mismatches: date-pattern values in columns that should never hold dates,
#' or non-date values in columns that should always be dates. Returns the name
#' of the column where the shift likely originated (i.e. where to start the
#' left-shift correction), or NA if no mismatch is found.
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
    # The earliest mismatched column tells us where shifted data landed.
    # Subtract n_extra (the number of extra tabs inserted) to find the
    # column where the spurious tab was inserted — that's where we start
    # shifting everything left.
    shift_idx <- min(violations) - n_extra
    if (shift_idx >= 1) col_names[shift_idx] else NA_character_
  }
}

#' Shift columns left in a single data.table row to undo a mid-row tab
#' insertion. Starting at `col_name`, every cell is replaced by the cell
#' `n_offset` positions to its right (effectively "deleting" the spurious
#' tab-created gaps). Expects dt to have a key column `n` for row lookup.
#' @param dt data.table with setkey(dt, n)
#' @param row_n integer row key (value of `n` column)
#' @param col_name character name of the first column to shift into
#' @param n_offset integer number of positions to shift left (= # of extra tabs)
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

#' Check that date columns parsed without excessive failures using
#' readr::problems(). Stops if the failure count exceeds a threshold.
#' @param df data.frame returned by type_convert()
#' @param max_fail_rate numeric threshold (0-1); default 0.001 = 0.1%
#' @param label optional label for error messages
check_date_parse <- function(df, max_fail_rate = 0.001, label = "data") {
  p <- problems(df)
  if (nrow(p) > 0L) {
    fail_rate <- nrow(p) / nrow(df)
    if (fail_rate > max_fail_rate) {
      stop(sprintf(
        "check_date_parse [%s]: %d parse failures (%.2f%% of %d rows). First few: %s",
        label,
        nrow(p),
        fail_rate * 100,
        nrow(df),
        paste(head(paste(p$col, p$expected, sep = ": "), 10), collapse = "; ")
      ))
    }
  }
  invisible(df)
}

#' Automatically detect and fix mid-row tab shifts in an fread result.
#'
#' @param dt data.table as read by fread (with fill = TRUE)
#' @param shift_col_finder function(row_dt, n_extra) -> column name or
#'   "CONCAT_THEN_<col>" signal, or NA_character_ if unfixable
#' @param pre_fix optional function(dt, row_n, n_extra) called before shifting
#'   when shift_col_finder returns a "CONCAT_THEN_" prefixed result
#' @return list with two elements:
#'   - dt: the fixed data.table (unfixable rows removed)
#'   - fixes: data.frame summarising each fix (row_n, n_extra, shift_col,
#'     pass, status)
auto_fix_tab_shifts <- function(dt, shift_col_finder, pre_fix = NULL) {
  has_content <- function(x) !is.na(x) & nchar(trimws(as.character(x))) > 0

  # Accumulator for fix metadata
  fix_log <- list()
  log_fix <- function(row_n, n_extra, shift_col, pass, status) {
    fix_log[[length(fix_log) + 1L]] <<- data.frame(
      row_n = row_n,
      n_extra = n_extra,
      shift_col = shift_col,
      pass = pass,
      status = status,
      stringsAsFactors = FALSE
    )
  }

  # Add row key
  dt[, n := .I]
  setkey(dt, n)

  overflow_cols <- grep("^V\\d+$", colnames(dt), value = TRUE)
  n_overflow <- length(overflow_cols)

  # --- Pass 1: fix rows with overflow columns ---
  # Detect by EITHER non-empty overflow content OR by having non-NA values

  # that were shifted into overflow positions (i.e. overflow cols exist and
  # the row's real columns show type violations). Empty overflow can happen
  # when the extra tabs pushed already-empty trailing columns off the end.
  shifted_rows <- integer(0)
  if (n_overflow > 0L) {
    # Flag rows where ANY overflow column has content
    shifted_mask <- rep(FALSE, nrow(dt))
    for (vc in overflow_cols) {
      shifted_mask <- shifted_mask | has_content(dt[[vc]])
    }
    # Also flag rows where type violations exist (date in non-date col)
    # even if the overflow columns are empty — this catches rows where
    # trailing fields were already blank so nothing spilled into overflow.
    finder_env <- environment(shift_col_finder)
    if (exists("non_date_cols", envir = finder_env)) {
      ndc <- intersect(
        get("non_date_cols", envir = finder_env),
        colnames(dt)
      )
      date_rx <- "^[0-9]{4}-[0-9]{2}-[0-9]{2}"
      for (col in ndc) {
        shifted_mask <- shifted_mask |
          grepl(date_rx, as.character(dt[[col]]))
      }
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
    # When overflow cols are all empty, we don't know n_extra from overflow
    # alone — try n_extra = 1 as a starting point and iterate
    if (n_extra == 0L) {
      n_extra <- 1L
    }
    shift_col <- shift_col_finder(row_dt, n_extra)
    if (is.na(shift_col)) {
      warning(sprintf(
        "auto_fix_tab_shifts: cannot fix row n=%d (n_extra=%d), dropping",
        rn,
        n_extra
      ))
      log_fix(rn, n_extra, NA_character_, "overflow", "dropped")
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
    log_fix(rn, n_extra, shift_col, "overflow", "fixed")
    shift_left_dt(dt, rn, shift_col, n_extra)

    # Re-check for remaining violations (multi-point shifts: extra tabs
    # inserted at two or more separate places in the same row). After fixing
    # the first shift, violations from subsequent insertions may remain.
    # Cap at 3 retries — no real row has more than a few independent insertions.
    for (retry in 1:3) {
      row_dt <- dt[.(rn)]
      retry_col <- shift_col_finder(row_dt, 1L)
      if (is.na(retry_col)) {
        break
      }
      if (grepl("^CONCAT_THEN_", retry_col)) {
        actual_col <- sub("^CONCAT_THEN_", "", retry_col)
        if (!is.null(pre_fix)) {
          pre_fix(dt, rn, 1L)
        }
        retry_col <- actual_col
      }
      log_fix(rn, 1L, retry_col, "overflow-multi", "fixed")
      shift_left_dt(dt, rn, retry_col, 1L)
    }
  }

  # --- Pass 2: fix "hidden" shifts (no overflow columns at all) ---
  # Some files have no overflow rows, but individual rows still have type
  # violations (e.g. a date in a non-date column) indicating a mid-row shift.
  # This pass catches shifts in files where max_fields == n_header.
  already_fixed <- c(shifted_rows, rows_to_drop)
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
        # Try n_extra = 1, 2, 3 — we don't know how many extra tabs were
        # inserted since there's no overflow to count, so we guess and check
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
            log_fix(rn, n_try, shift_col, "hidden", "fixed")
            shift_left_dt(dt, rn, shift_col, n_try)

            # Re-check for remaining violations (multi-point shifts).
            # Cap at 3 — no real row has more than a few independent insertions.
            for (retry in 1:3) {
              row_dt2 <- dt[.(rn)]
              retry_col <- shift_col_finder(row_dt2, 1L)
              if (is.na(retry_col)) {
                break
              }
              if (grepl("^CONCAT_THEN_", retry_col)) {
                actual_col <- sub("^CONCAT_THEN_", "", retry_col)
                if (!is.null(pre_fix)) {
                  pre_fix(dt, rn, 1L)
                }
                retry_col <- actual_col
              }
              log_fix(rn, 1L, retry_col, "hidden-multi", "fixed")
              shift_left_dt(dt, rn, retry_col, 1L)
            }
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

  fixes <- if (length(fix_log) > 0L) {
    do.call(rbind, fix_log)
  } else {
    data.frame(
      row_n = integer(),
      n_extra = integer(),
      shift_col = character(),
      pass = character(),
      status = character(),
      stringsAsFactors = FALSE
    )
  }

  message(sprintf(
    "auto_fix_tab_shifts summary: %d fixed, %d dropped",
    sum(fixes$status == "fixed"),
    sum(fixes$status == "dropped")
  ))

  list(dt = dt, fixes = fixes)
}
