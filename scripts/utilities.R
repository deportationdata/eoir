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
  # Count tab-separated fields on header vs max on any row
  n_header <- as.integer(system(
    sprintf("awk -F'\t' 'NR==1{print NF; exit}' %s", shQuote(file)),
    intern = TRUE
  ))
  max_fields <- as.integer(system(
    sprintf("awk -F'\t' 'BEGIN{m=0} NF>m{m=NF} END{print m}' %s", shQuote(file)),
    intern = TRUE
  ))

  # When rows have more fields than the header, pad the header with dummy
  # column names so fread sees a consistent field count.
  needs_padding <- max_fields > n_header
  if (needs_padding) {
    header_line <- readLines(file, n = 1L, warn = FALSE)
    extra_names <- paste0("V", seq_len(max_fields - n_header))
    padded_header <- paste0(header_line, "\t", paste(extra_names, collapse = "\t"))
    tmp <- tempfile(fileext = ".tsv")
    on.exit(unlink(tmp), add = TRUE)
    system(sprintf(
      "{ echo %s; tail -n +2 %s; } > %s",
      shQuote(padded_header), shQuote(file), shQuote(tmp)
    ))
  }

  # Overflow files use na.strings="" to preserve values like "N/A" that
  # auto_fix_tab_shifts needs for shifted-row detection; clean_eoir_cols()
  # converts NA-like strings after fixing.
  dt <- fread(
    if (needs_padding) tmp else file,
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = if (needs_padding) "" else c("", "NA", "N/A", "NULL"),
    colClasses = if (needs_padding) "character" else col_types,
    fill = TRUE,
    showProgress = FALSE
  )

  # Validate row count against Count.txt if present
  tbl_name <- tools::file_path_sans_ext(basename(file))
  count_file <- file.path(dirname(file), "Count.txt")
  if (file.exists(count_file)) {
    count_lines <- read_lines(count_file)
    expected <- count_lines |>
      keep(~ str_detect(., paste0("^", tbl_name, "\\t"))) |>
      str_extract("(?<=\\t)\\d+$") |>
      as.integer()
    if (length(expected) == 1L && !is.na(expected)) {
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
#' and convert NA-like strings to real NAs.
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

#' Build a shift finder that detects mid-row column shifts by looking for
#' type mismatches: date-pattern values in columns that should never hold dates,
#' or non-date values in columns that should always be dates. Returns the name
#' of the column where the shift likely originated, or NA if no mismatch.
make_shift_finder <- function(date_cols, non_date_cols) {
  date_pat <- "^\\d{4}-\\d{2}-\\d{2}"
  function(row_dt, n_extra) {
    col_names <- colnames(row_dt)
    violations <- integer(0)
    for (col in date_cols) {
      if (!col %in% col_names) next
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && nchar(val) > 0 && !grepl(date_pat, val)) {
        violations <- c(violations, which(col_names == col))
      }
    }
    for (col in non_date_cols) {
      if (!col %in% col_names) next
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && grepl(date_pat, val)) {
        violations <- c(violations, which(col_names == col))
      }
    }
    if (!length(violations)) return(NA_character_)
    shift_idx <- min(violations) - n_extra
    if (shift_idx >= 1) col_names[shift_idx] else NA_character_
  }
}

#' Shift columns left in a single data.table row to undo a mid-row tab
#' insertion. Starting at `col_name`, every cell is replaced by the cell
#' `n_offset` positions to its right.
shift_left_dt <- function(dt, row_n, col_name, n_offset) {
  c_idx <- which(colnames(dt) == col_name)
  i <- dt[.(row_n), which = TRUE]
  last_col <- ncol(dt)
  if ("n" %in% colnames(dt)) last_col <- which(colnames(dt) == "n") - 1L
  for (j in seq(c_idx, last_col)) {
    j_offset <- j + n_offset
    set(dt, i, j, if (j_offset <= last_col) dt[i, ..j_offset] else NA_character_)
  }
  invisible(dt)
}

#' Check that date columns parsed without excessive failures.
check_date_parse <- function(df, max_fail_rate = 0.001, label = "data") {
  p <- problems(df)
  if (nrow(p) > 0L) {
    fail_rate <- nrow(p) / nrow(df)
    if (fail_rate > max_fail_rate) {
      stop(sprintf(
        "check_date_parse [%s]: %d parse failures (%.2f%% of %d rows). First few: %s",
        label, nrow(p), fail_rate * 100, nrow(df),
        paste(head(paste(p$col, p$expected, sep = ": "), 10), collapse = "; ")
      ))
    }
  }
  invisible(df)
}

#' Automatically detect and fix mid-row tab shifts in an fread result.
#' Returns list(dt, fixes).
auto_fix_tab_shifts <- function(dt, shift_col_finder, pre_fix = NULL) {
  has_content <- function(x) !is.na(x) & nchar(trimws(as.character(x))) > 0
  date_pat <- "^\\d{4}-\\d{2}-\\d{2}"

  # Extract check columns from the shift finder's closure
  finder_env <- environment(shift_col_finder)
  date_cols <- mget("date_cols", envir = finder_env, ifnotfound = list(character(0)))[[1]]
  non_date_cols <- mget("non_date_cols", envir = finder_env, ifnotfound = list(character(0)))[[1]]
  check_cols <- intersect(union(date_cols, non_date_cols), colnames(dt))

  # Check a single row for type violations, returns description strings
  find_violations <- function(row_dt) {
    v <- character(0)
    for (col in date_cols) {
      if (!col %in% names(row_dt)) next
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && nchar(val) > 0 && !grepl(date_pat, val)) {
        v <- c(v, paste0(col, "='", val, "' (expected date)"))
      }
    }
    for (col in non_date_cols) {
      if (!col %in% names(row_dt)) next
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && grepl(date_pat, val)) {
        v <- c(v, paste0(col, "='", val, "' (unexpected date)"))
      }
    }
    v
  }

  snapshot_row <- function(row_dt) {
    vapply(check_cols, function(c) as.character(row_dt[[c]]), character(1))
  }

  # Resolve CONCAT_THEN_ prefix, apply pre_fix if needed, shift left,
  # then retry up to 3 times for multi-point shifts. Returns resolved col name.
  fix_one_row <- function(rn, shift_col, n_extra) {
    if (grepl("^CONCAT_THEN_", shift_col)) {
      shift_col <- sub("^CONCAT_THEN_", "", shift_col)
      if (!is.null(pre_fix)) pre_fix(dt, rn, n_extra)
    }
    shift_left_dt(dt, rn, shift_col, n_extra)
    for (retry in 1:3) {
      retry_col <- shift_col_finder(dt[.(rn)], 1L)
      if (is.na(retry_col)) break
      if (grepl("^CONCAT_THEN_", retry_col)) {
        retry_col <- sub("^CONCAT_THEN_", "", retry_col)
        if (!is.null(pre_fix)) pre_fix(dt, rn, 1L)
      }
      shift_left_dt(dt, rn, retry_col, 1L)
    }
    shift_col
  }

  # Build mask of rows where non-date columns contain date-like values
  date_in_non_date_mask <- function() {
    ndc <- intersect(non_date_cols, colnames(dt))
    mask <- rep(FALSE, nrow(dt))
    for (col in ndc) {
      vals <- as.character(dt[[col]])
      mask <- mask | (!is.na(vals) & grepl("^[0-9]{4}-[0-9]{2}-[0-9]{2}", vals))
    }
    mask
  }

  # Fix-log accumulator
  fix_log <- list()
  log_fix <- function(row_n, n_extra, shift_col, pass, status,
                      orig_violations, orig_vals, fixed_violations, fixed_vals) {
    fix_log[[length(fix_log) + 1L]] <<- data.frame(
      row_n = row_n, n_extra = n_extra, shift_col = shift_col,
      pass = pass, status = status,
      original_violations = if (length(orig_violations)) paste(orig_violations, collapse = "; ") else NA_character_,
      original_vals = paste(names(orig_vals), orig_vals, sep = "=", collapse = "; "),
      fixed_violations = if (length(fixed_violations)) paste(fixed_violations, collapse = "; ") else NA_character_,
      fixed_vals = paste(names(fixed_vals), fixed_vals, sep = "=", collapse = "; "),
      stringsAsFactors = FALSE
    )
  }

  initial_nrow <- nrow(dt)
  dt[, n := .I]
  setkey(dt, n)

  overflow_cols <- grep("^V\\d+$", colnames(dt), value = TRUE)

  # --- Pass 1: fix rows with overflow columns ---
  shifted_rows <- integer(0)
  if (length(overflow_cols) > 0L) {
    shifted_mask <- rep(FALSE, nrow(dt))
    for (vc in overflow_cols) shifted_mask <- shifted_mask | has_content(dt[[vc]])
    shifted_mask <- shifted_mask | date_in_non_date_mask()
    shifted_rows <- dt[shifted_mask, n]
  }

  if (length(shifted_rows) > 0L) {
    message(sprintf("auto_fix_tab_shifts: %d overflow-shifted rows detected", length(shifted_rows)))
  }

  # Snapshot untouched rows for later validation
  all_candidate <- setdiff(seq_len(nrow(dt)), shifted_rows)
  n_sample <- min(5000L, length(all_candidate))
  sample_ns <- integer(0)
  if (n_sample > 0L) {
    sample_ns <- sort(sample(all_candidate, n_sample))
    untouched_snapshot <- copy(dt[sample_ns, ..check_cols])
  }

  rows_to_drop <- integer(0)

  for (rn in shifted_rows) {
    row_dt <- dt[.(rn)]
    orig_v <- find_violations(row_dt)
    orig_s <- snapshot_row(row_dt)

    n_extra <- sum(vapply(overflow_cols, function(vc) has_content(row_dt[[vc]]), logical(1)))
    detected_from_overflow <- n_extra > 0L
    if (n_extra == 0L) n_extra <- 1L

    shift_col <- shift_col_finder(row_dt, n_extra)
    if (is.na(shift_col)) {
      if (!detected_from_overflow) next
      warning(sprintf("auto_fix_tab_shifts: cannot fix row n=%d (n_extra=%d), dropping", rn, n_extra))
      log_fix(rn, n_extra, NA_character_, "overflow", "dropped",
              orig_v, orig_s, character(0), snapshot_row(dt[.(rn)]))
      rows_to_drop <- c(rows_to_drop, rn)
      next
    }

    resolved_col <- fix_one_row(rn, shift_col, n_extra)

    fixed_row <- dt[.(rn)]
    log_fix(rn, n_extra, resolved_col, "overflow", "fixed",
            orig_v, orig_s, find_violations(fixed_row), snapshot_row(fixed_row))
  }

  # --- Pass 2: fix "hidden" shifts (no overflow columns) ---
  already_fixed <- c(shifted_rows, rows_to_drop)
  suspect_mask <- date_in_non_date_mask()
  suspect_mask[dt$n %in% already_fixed] <- FALSE
  suspect_rows <- dt[suspect_mask, n]

  if (length(suspect_rows) > 0L) {
    for (n_try in 1:3) {
      hidden_rows <- integer(0)
      for (rn in suspect_rows) {
        if (!is.na(shift_col_finder(dt[.(rn)], n_try))) {
          hidden_rows <- c(hidden_rows, rn)
        }
      }
      if (length(hidden_rows) == 0L) next

      message(sprintf(
        "auto_fix_tab_shifts: %d hidden-shifted rows detected (n_extra=%d)",
        length(hidden_rows), n_try
      ))

      for (rn in hidden_rows) {
        row_dt <- dt[.(rn)]
        orig_v <- find_violations(row_dt)
        orig_s <- snapshot_row(row_dt)

        shift_col <- shift_col_finder(row_dt, n_try)
        resolved_col <- fix_one_row(rn, shift_col, n_try)

        fixed_row <- dt[.(rn)]
        log_fix(rn, n_try, resolved_col, "hidden", "fixed",
                orig_v, orig_s, find_violations(fixed_row), snapshot_row(fixed_row))
      }
      suspect_rows <- setdiff(suspect_rows, hidden_rows)
      already_fixed <- c(already_fixed, hidden_rows)
    }
  }

  # --- Cleanup ---
  if (length(rows_to_drop) > 0L) {
    message(sprintf("auto_fix_tab_shifts: dropped %d unfixable rows", length(rows_to_drop)))
    dt <- dt[!n %in% rows_to_drop]
  }
  dt[, n := NULL]

  fixes <- if (length(fix_log) > 0L) {
    do.call(rbind, fix_log)
  } else {
    data.frame(
      row_n = integer(), n_extra = integer(), shift_col = character(),
      pass = character(), status = character(),
      original_violations = character(), original_vals = character(),
      fixed_violations = character(), fixed_vals = character(),
      stringsAsFactors = FALSE
    )
  }

  n_fixed <- sum(fixes$status == "fixed")
  n_dropped <- sum(fixes$status == "dropped")
  message(sprintf("auto_fix_tab_shifts summary: %d fixed, %d dropped", n_fixed, n_dropped))

  # --- Validation ---
  n_total_touched <- length(unique(fixes$row_n))
  if (n_total_touched > 50L) {
    stop(sprintf(
      "auto_fix_tab_shifts validation FAILED: %d rows edited, exceeds 50-row safety limit",
      n_total_touched
    ))
  }

  final_nrow <- nrow(dt)
  expected_nrow <- initial_nrow - n_dropped
  if (final_nrow != expected_nrow) {
    stop(sprintf(
      "auto_fix_tab_shifts validation FAILED: row count — started %d, dropped %d, expected %d, got %d",
      initial_nrow, n_dropped, expected_nrow, final_nrow
    ))
  }
  message(sprintf("auto_fix_tab_shifts validation: row count OK (%d - %d dropped = %d)",
                  initial_nrow, n_dropped, final_nrow))

  if (n_fixed > 0L) {
    fixed_fixes <- fixes[fixes$status == "fixed", ]

    no_orig_violation <- is.na(fixed_fixes$original_violations)
    if (any(no_orig_violation)) {
      stop(sprintf(
        "auto_fix_tab_shifts validation FAILED: %d 'fixed' rows had NO original violation: rows %s",
        sum(no_orig_violation),
        paste(head(fixed_fixes$row_n[no_orig_violation], 10), collapse = ", ")
      ))
    }

    still_bad <- !is.na(fixed_fixes$fixed_violations)
    if (any(still_bad)) {
      stop(sprintf(
        "auto_fix_tab_shifts validation FAILED: %d rows still have violations: rows %s\n  Details: %s",
        sum(still_bad),
        paste(head(fixed_fixes$row_n[still_bad], 10), collapse = ", "),
        paste(head(fixed_fixes$fixed_violations[still_bad], 5), collapse = "; ")
      ))
    }

    message(sprintf("auto_fix_tab_shifts validation: all %d fixes verified", nrow(fixed_fixes)))
  }

  if (length(sample_ns) > 0L) {
    all_touched <- unique(fixes$row_n)
    keep_mask <- !sample_ns %in% all_touched
    if (any(keep_mask)) {
      verify_ns <- sample_ns[keep_mask]
      verify_snapshot <- untouched_snapshot[keep_mask, ]
      if (n_dropped > 0L) {
        dropped_ns <- fixes$row_n[fixes$status == "dropped"]
        kept <- setdiff(seq_len(initial_nrow), dropped_ns)
        verify_fixed_pos <- match(verify_ns, kept)
      } else {
        verify_fixed_pos <- verify_ns
      }
      current <- dt[verify_fixed_pos, ..check_cols]
      diffs <- !mapply(
        function(a, b) identical(as.character(a), as.character(b)),
        verify_snapshot, current
      )
      if (any(diffs)) {
        stop(sprintf(
          "auto_fix_tab_shifts validation FAILED: untouched rows altered in columns: %s",
          paste(check_cols[diffs], collapse = ", ")
        ))
      }
      message(sprintf(
        "auto_fix_tab_shifts validation: %d sampled untouched rows verified unchanged",
        length(verify_ns)
      ))
    }
  }

  list(dt = dt, fixes = fixes)
}
