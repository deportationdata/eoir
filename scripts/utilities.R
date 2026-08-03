# Shared utilities for EOIR CSV cleaning
# Handles: control characters, extra tabs (mid-row shifts & end-of-row overflow)

# data.table is deliberately NOT attached: it is loaded via `data.table::`
# below instead. `utilities.R` is sourced *after* each script's
# `library(duckplyr)`, so attaching it here would put it last on the search
# path and mask `dplyr::first()`, `last()` and `between()` pipeline-wide —
# and `data.table::last(character(0))` returns `character(0)` where
# `dplyr::last()` returns NA. `[.data.table`, `:=`, `.I` and `..col` dispatch
# on object class rather than attachment, so they keep working unqualified.
library(stringr)
library(duckplyr)
library(readr)
library(pointblank)

na_vals <- c("")

#' Keep DuckDB from competing with R for the last of the machine's memory.
#'
#' DuckDB sizes its own budget at 80% of total system RAM and has no idea that
#' the R session driving it is holding the same tables at the same time. On
#' B_TblProceeding (16.6M rows) the two together exhausted the CI runner and the
#' VM was killed mid-job — the job log ends at "The runner has received a
#' shutdown signal" with no R error, because nothing in R got the chance to
#' raise one.
#'
#' So give DuckDB a fixed share and somewhere to spill. An in-memory DuckDB
#' database does not page to disk at all unless `temp_directory` is set: without
#' it, hitting the limit is a hard error rather than a slower query.
configure_duckdb_memory <- function(fraction = 0.5, temp_dir = "tmp/duckdb") {
  meminfo <- tryCatch(readLines("/proc/meminfo"), error = function(e) NULL)
  total_line <- grep("^MemTotal:", meminfo, value = TRUE)

  if (length(total_line) == 1L) {
    total_gb <- as.numeric(sub("\\D*(\\d+).*", "\\1", total_line)) / 1024^2
    limit_gb <- max(1, floor(fraction * total_gb))
    db_exec(sprintf("SET memory_limit = '%dGB'", limit_gb))
    message(sprintf(
      "duckdb: memory_limit %dGB of %.0fGB total, spilling to %s",
      limit_gb,
      total_gb,
      temp_dir
    ))
  }

  dir.create(temp_dir, recursive = TRUE, showWarnings = FALSE)
  db_exec(sprintf("SET temp_directory = '%s'", temp_dir))
}

configure_duckdb_memory()

#' Run a pointblank validation chain inside DuckDB instead of in R.
#'
#' pointblank has no duckplyr backend — a duckplyr frame inherits data.frame,
#' so every validation is computed in R over the whole table. On a 3M x 19
#' frame that is 20.8s and +584MB of R heap per chain; against the same data as
#' a DuckDB table it is 13.7s and no measurable heap growth, because pointblank
#' pushes `tbl_dbi` validations down to SQL. Pass/fail counts are identical, so
#' the warn/stop thresholds behave exactly as before.
#'
#' A lazy duckplyr frame is handed over with compute_parquet(), which writes it
#' from DuckDB without building an R copy. That matters: as.data.frame() on the
#' ~180-column `cases` frame cost 30GB of R heap and was what pushed the runner
#' over. Via parquet the same validation costs a couple of hundred MB.
#'
#' duckplyr's own relation would be the tidier bridge, but last_rel() is a
#' debugging aid and returns a *stale* relation — on a frame built with
#' filter() then mutate() it reported only the pre-mutate columns, which would
#' silently validate the wrong data. compute_parquet() reflects the frame as it
#' actually is.
#'
#' An already-materialized frame skips the round trip and is registered
#' directly, which duckdb does zero-copy.
#'
#' Usage mirrors the in-place form, and the input is returned unchanged:
#'   validate_in_duckdb(cases, \(tbl) tbl |> col_vals_not_null(idncase))
#'
#' Only for validations DuckDB can express. `col_vals_expr()` takes an R
#' expression and is deliberately left running in R. Note also that anything
#' relying on nrow() sees NA through a lazy handle — see row_count_match().
validate_in_duckdb <- function(df, chain) {
  con <- DBI::dbConnect(duckdb::duckdb())
  on.exit(DBI::dbDisconnect(con, shutdown = TRUE), add = TRUE)

  if (inherits(df, "duckplyr_df")) {
    dir.create("tmp", showWarnings = FALSE)
    path <- tempfile("validate_", tmpdir = "tmp", fileext = ".parquet")
    on.exit(unlink(path), add = TRUE)

    duckplyr::compute_parquet(df, path)
    tbl <- dplyr::tbl(
      con,
      dplyr::sql(sprintf(
        "SELECT * FROM read_parquet(%s)",
        DBI::dbQuoteString(con, path)
      ))
    )
  } else {
    duckdb::duckdb_register(con, "pb_tbl", as.data.frame(df))
    tbl <- dplyr::tbl(con, "pb_tbl")
  }

  chain(tbl)

  invisible(df)
}

#' Announce which block of a script is running, with the resident size of the
#' R session. The workflow prints machine-wide memory alongside this, so
#' between the two a run that dies without an R error still says where and on
#' what.
log_step <- function(label) {
  rss_mb <- tryCatch(
    {
      status <- readLines(sprintf("/proc/%d/status", Sys.getpid()))
      as.numeric(sub("\\D*(\\d+).*", "\\1", grep("^VmRSS:", status, value = TRUE))) / 1024
    },
    error = function(e) NA_real_
  )
  message(sprintf(
    "[step] %s (R rss %s MB)",
    label,
    if (length(rss_mb) == 1L && !is.na(rss_mb)) format(round(rss_mb)) else "?"
  ))
}

#' Read an EOIR TSV file with standardized parameters and row-count validation.
#' Returns a data.table.
read_eoir_tsv <- function(file) {
  # Count tab-separated fields on header vs max on any row
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

  # When rows have more fields than the header, pad the header with dummy
  # column names so fread sees a consistent field count.
  needs_padding <- max_fields > n_header
  if (needs_padding) {
    header_line <- readLines(file, n = 1L, warn = FALSE)
    extra_names <- paste0("V", seq_len(max_fields - n_header))
    padded_header <- paste0(
      header_line,
      "\t",
      paste(extra_names, collapse = "\t")
    )
    tmp <- tempfile(fileext = ".tsv")
    on.exit(unlink(tmp), add = TRUE)
    system(sprintf(
      "{ echo %s; tail -n +2 %s; } > %s",
      shQuote(padded_header),
      shQuote(file),
      shQuote(tmp)
    ))
  }

  # Use na.strings="" to preserve values like "N/A" that
  # auto_fix_tab_shifts needs for shifted-row detection; clean_eoir_cols()
  # converts NA-like strings after fixing.
  dt <- data.table::fread(
    if (needs_padding) tmp else file,
    sep = "\t",
    quote = "",
    header = TRUE,
    na.strings = "",
    colClasses = "character",
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
  raw_bytes <- readBin(file, "raw", file.info(file)$size)
  raw_bytes <- raw_bytes[raw_bytes != as.raw(0L)]
  read_delim(
    I(rawToChar(raw_bytes)),
    delim = "\t",
    col_types = cols(.default = col_character()),
    na = c(""),
    show_col_types = FALSE
  ) |>
    janitor::clean_names() |>
    mutate(across(where(is.character), str_squish))
}

#' Cells that need whitespace/control-character repair: a control character
#' (tab, newline, CR), a leading or trailing space, a doubled space, or any
#' whitespace that is not a plain space (e.g. a non-breaking space).
DIRTY_CELL_REGEX <- "[[:cntrl:]]|^\\s|\\s$|\\s\\s|[^\\S ]"

#' Remove control characters, normalise whitespace, drop overflow columns,
#' and convert NA-like strings to real NAs.
#'
#' Every column is still character when this runs (read_eoir_tsv() uses
#' colClasses = "character"), so this touches roughly 700M cells across the
#' seven table scripts and is the most expensive step in the pipeline. Rather
#' than rewriting every cell, it detects the ones that actually need repair and
#' rewrites only those, on the expectation that most EOIR values are
#' already-clean ids, codes and dates.
#'
#' That trade is not free: the detection pass is pure overhead on cells that
#' turn out to be dirty. Measured on 600k x 20, break-even is around 85% dirty
#' — 3.7x faster at 5%, 2.4x at 20%, 1.3x at 60%, but 0.89x if essentially
#' every cell needs work. The dirty rate is reported below so the assumption
#' can be checked against real input rather than assumed.
clean_eoir_cols <- function(df) {
  n_dirty <- 0
  n_seen <- 0

  repair <- function(x) {
    hit <- stringi::stri_detect_regex(x, DIRTY_CELL_REGEX)
    hit[is.na(hit)] <- FALSE
    n_dirty <<- n_dirty + sum(hit)
    n_seen <<- n_seen + length(hit)
    if (any(hit)) {
      # Note control characters are *removed*, not turned into spaces, so
      # "a<tab>b" becomes "ab" — matching the str_remove_all() + str_squish()
      # behaviour this replaces.
      x[hit] <- stringi::stri_replace_all_regex(
        x[hit],
        c("[[:cntrl:]]", "\\s+", "^ | $"),
        c("", " ", ""),
        vectorize_all = FALSE
      )
    }
    x[!is.na(x) & x == ""] <- NA_character_
    x
  }

  # Column-at-a-time assignment rather than mutate(across()) + select().
  # On B_TblProceeding (16.6M rows x 40 all-character columns) across() holds
  # every repaired column alongside every original one before assigning, so
  # peak memory is two full copies of the table; the loop keeps one column in
  # flight and lets the previous original be collected. Dropping the overflow
  # columns by name likewise avoids handing the whole table to DuckDB and
  # reading it back just to remove a few columns.
  out <- df[, !grepl("^V\\d+$", names(df)), drop = FALSE]
  for (nm in names(out)) {
    if (is.character(out[[nm]])) {
      out[[nm]] <- repair(out[[nm]])
    }
  }

  if (n_seen > 0L) {
    message(sprintf(
      "clean_eoir_cols: repaired %s of %s character cells (%.2f%%)%s",
      format(n_dirty, big.mark = ","),
      format(n_seen, big.mark = ","),
      100 * n_dirty / n_seen,
      if (n_dirty / n_seen > 0.85) " — above the ~85% break-even, this step would be faster rewriting every cell" else ""
    ))
  }

  out
}

#' Fix known abbreviations back to their canonical uppercase form after
#' str_to_title() has title-cased them (e.g. "Bia" -> "BIA", "Ij" -> "IJ").
#' Applies all abbreviations in a single vectorized regex pass (via stringi's
#' parallel pattern/replacement matching) instead of one str_replace_all()
#' pass per abbreviation — same output, dramatically faster on large tables.
str_fix_abbreviations <- function(x, abbr) {
  pat <- ifelse(grepl("/", abbr, fixed = TRUE), abbr, paste0("\\b", abbr, "\\b"))
  stringi::stri_replace_all_regex(
    x,
    pat,
    abbr,
    opts_regex = stringi::stri_opts_regex(case_insensitive = TRUE),
    vectorize_all = FALSE
  )
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

#' Shift columns left in a single data.table row to undo a mid-row tab
#' insertion. Starting at `col_name`, every cell is replaced by the cell
#' `n_offset` positions to its right.
shift_left_dt <- function(dt, row_n, col_name, n_offset) {
  c_idx <- which(colnames(dt) == col_name)
  i <- dt[.(row_n), which = TRUE]
  last_col <- ncol(dt)
  if ("n" %in% colnames(dt)) {
    last_col <- which(colnames(dt) == "n") - 1L
  }
  for (j in seq(c_idx, last_col)) {
    j_offset <- j + n_offset
    data.table::set(
      dt,
      i,
      j,
      if (j_offset <= last_col) dt[i, ..j_offset] else NA_character_
    )
  }
  invisible(dt)
}

#' Fast type conversion with problem tracking.
#' Replaces type_convert() + check_parse() — uses base R coercion and
#' tracks new NAs introduced by failed conversions.
fast_convert <- function(df, col_specs, na = na_vals, max_fail_rate = 0.001) {
  n <- nrow(df)
  bad_cols <- character(0)

  for (col_name in names(col_specs)) {
    orig <- df[[col_name]]
    was_na <- is.na(orig) | orig %in% na

    df[[col_name]] <- switch(
      col_specs[[col_name]],
      integer = suppressWarnings(as.integer(orig)),
      double = suppressWarnings(as.double(orig)),
      # readr's parser rather than as.POSIXct(): ~9x faster (8.6s -> 1.0s per
      # 2M-row column), and the dominant cost in this function.
      #
      # `format` is required, not stylistic. Left to guess, parse_datetime()
      # is more permissive than as.POSIXct() — it accepts a bare "2020-01-01"
      # where as.POSIXct() returns NA. That would both change published values
      # and shift the failure counts checked against max_fail_rate below. With
      # the format pinned, the NA pattern and the parsed values match
      # as.POSIXct() exactly across valid input, "", NA, "N/A", malformed
      # dates and whitespace-padded values.
      datetime = {
        parsed <- suppressWarnings(
          readr::parse_datetime(orig, format = "%Y-%m-%d %H:%M:%S")
        )
        # readr attaches a `problems` attribute recording every failed row.
        # It survives downstream conversions such as as.Date() and would ride
        # into the written parquet, so drop it — this function does its own
        # failure accounting below.
        attr(parsed, "problems") <- NULL
        parsed
      },
      # Deliberately NOT readr::parse_date(): it returns NA for
      # "2020-01-01 00:00:00", which as.Date() accepts.
      date = as.Date(orig, format = "%Y-%m-%d"),
      stop(sprintf(
        "fast_convert: unknown type '%s' for column '%s'",
        col_specs[[col_name]],
        col_name
      ))
    )

    new_na <- is.na(df[[col_name]]) & !was_na
    fail_n <- sum(new_na)

    if (fail_n > 0L) {
      rate <- fail_n / n
      bad_vals <- paste(head(unique(orig[new_na]), 5), collapse = ", ")
      if (rate > max_fail_rate) {
        bad_cols <- c(
          bad_cols,
          sprintf(
            "%s: %d (%.2f%%) e.g. %s",
            col_name,
            fail_n,
            rate * 100,
            bad_vals
          )
        )
      } else {
        message(sprintf(
          "fast_convert: %s — %d failures (%.4f%%), under threshold. e.g. %s",
          col_name,
          fail_n,
          rate * 100,
          bad_vals
        ))
      }
    }
  }

  if (length(bad_cols) > 0L) {
    stop(sprintf(
      "fast_convert: parse failures exceed %.3f%% in %d column(s): %s",
      max_fail_rate * 100,
      length(bad_cols),
      paste(bad_cols, collapse = "; ")
    ))
  }

  df
}

#' Check that date columns parsed without excessive failures.
check_parse <- function(df, max_fail_rate = 0.001) {
  p <- problems(df)
  if (nrow(p) == 0L) {
    return(invisible(df))
  }

  n <- nrow(df)
  overall_rate <- nrow(p) / n

  col_summary <- p |>
    count(col) |>
    mutate(rate = .data$n / .env$n)

  bad_cols <- col_summary |> filter(rate > max_fail_rate)

  if (nrow(bad_cols) > 0L) {
    col_details <- paste(
      sprintf("%s: %d (%.2f%%)", bad_cols$col, bad_cols$n, bad_cols$rate * 100),
      collapse = "; "
    )
    stop(sprintf(
      "check_parse: parse failures exceed %.3f%% in %d column(s): %s",
      max_fail_rate * 100,
      nrow(bad_cols),
      col_details
    ))
  }

  if (overall_rate > 0) {
    message(sprintf(
      "check_parse: %d parse failures (%.4f%%) across %d column(s) — under threshold",
      nrow(p),
      overall_rate * 100,
      nrow(col_summary)
    ))
  }

  invisible(df)
}

#' Assert that a data frame has exactly the expected number of rows.
#' Designed for pipe use after joins that should not change row count.
row_count_match <- function(df, expected_n) {
  actual_n <- nrow(df)
  if (is.na(actual_n)) {
    # nrow() is NA for a lazy table — a dbplyr/DBI handle does not know its own
    # row count without querying. Left alone that made the comparison below
    # NA and failed with "missing value where TRUE/FALSE needed", naming
    # neither the table nor the count. Ask the database instead.
    actual_n <- dplyr::pull(dplyr::count(df, name = "..n_rows"), "..n_rows")
  }
  if (actual_n != expected_n) {
    stop(sprintf(
      "row_count_match: expected %d rows, got %d (diff=%d)",
      expected_n,
      actual_n,
      abs(actual_n - expected_n)
    ))
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
  date_cols <- mget(
    "date_cols",
    envir = finder_env,
    ifnotfound = list(character(0))
  )[[1]]
  non_date_cols <- mget(
    "non_date_cols",
    envir = finder_env,
    ifnotfound = list(character(0))
  )[[1]]
  check_cols <- intersect(union(date_cols, non_date_cols), colnames(dt))

  # Check a single row for type violations, returns description strings
  find_violations <- function(row_dt) {
    v <- character(0)
    for (col in date_cols) {
      if (!col %in% names(row_dt)) {
        next
      }
      val <- trimws(as.character(row_dt[[col]]))
      if (!is.na(val) && nchar(val) > 0 && !grepl(date_pat, val)) {
        v <- c(v, paste0(col, "='", val, "' (expected date)"))
      }
    }
    for (col in non_date_cols) {
      if (!col %in% names(row_dt)) {
        next
      }
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
      if (is.na(retry_col)) {
        break
      }
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
  log_fix <- function(
    row_n,
    n_extra,
    shift_col,
    pass,
    status,
    orig_violations,
    orig_vals,
    fixed_violations,
    fixed_vals
  ) {
    fix_log[[length(fix_log) + 1L]] <<- data.frame(
      row_n = row_n,
      n_extra = n_extra,
      shift_col = shift_col,
      pass = pass,
      status = status,
      original_violations = if (length(orig_violations)) {
        paste(orig_violations, collapse = "; ")
      } else {
        NA_character_
      },
      original_vals = paste(
        names(orig_vals),
        orig_vals,
        sep = "=",
        collapse = "; "
      ),
      fixed_violations = if (length(fixed_violations)) {
        paste(fixed_violations, collapse = "; ")
      } else {
        NA_character_
      },
      fixed_vals = paste(
        names(fixed_vals),
        fixed_vals,
        sep = "=",
        collapse = "; "
      ),
      stringsAsFactors = FALSE
    )
  }

  initial_nrow <- nrow(dt)
  dt[, n := .I]
  data.table::setkey(dt, n)

  overflow_cols <- grep("^V\\d+$", colnames(dt), value = TRUE)

  # --- Pass 1: fix rows with overflow columns ---
  shifted_rows <- integer(0)
  if (length(overflow_cols) > 0L) {
    shifted_mask <- rep(FALSE, nrow(dt))
    for (vc in overflow_cols) {
      shifted_mask <- shifted_mask | has_content(dt[[vc]])
    }
    shifted_mask <- shifted_mask | date_in_non_date_mask()
    shifted_rows <- dt[shifted_mask, n]
  }

  if (length(shifted_rows) > 0L) {
    message(sprintf(
      "auto_fix_tab_shifts: %d overflow-shifted rows detected",
      length(shifted_rows)
    ))
  }

  # Snapshot untouched rows for later validation
  all_candidate <- setdiff(seq_len(nrow(dt)), shifted_rows)
  n_sample <- min(5000L, length(all_candidate))
  sample_ns <- integer(0)
  if (n_sample > 0L) {
    sample_ns <- sort(sample(all_candidate, n_sample))
    untouched_snapshot <- data.table::copy(dt[sample_ns, ..check_cols])
  }

  rows_to_drop <- integer(0)

  for (rn in shifted_rows) {
    row_dt <- dt[.(rn)]
    orig_v <- find_violations(row_dt)
    orig_s <- snapshot_row(row_dt)

    n_extra <- sum(vapply(
      overflow_cols,
      function(vc) has_content(row_dt[[vc]]),
      logical(1)
    ))
    detected_from_overflow <- n_extra > 0L
    if (n_extra == 0L) {
      n_extra <- 1L
    }

    shift_col <- shift_col_finder(row_dt, n_extra)
    if (is.na(shift_col)) {
      if (!detected_from_overflow) {
        next
      }
      warning(sprintf(
        "auto_fix_tab_shifts: cannot fix row n=%d (n_extra=%d), dropping",
        rn,
        n_extra
      ))
      log_fix(
        rn,
        n_extra,
        NA_character_,
        "overflow",
        "dropped",
        orig_v,
        orig_s,
        character(0),
        snapshot_row(dt[.(rn)])
      )
      rows_to_drop <- c(rows_to_drop, rn)
      next
    }

    resolved_col <- fix_one_row(rn, shift_col, n_extra)

    fixed_row <- dt[.(rn)]
    log_fix(
      rn,
      n_extra,
      resolved_col,
      "overflow",
      "fixed",
      orig_v,
      orig_s,
      find_violations(fixed_row),
      snapshot_row(fixed_row)
    )
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
        orig_v <- find_violations(row_dt)
        orig_s <- snapshot_row(row_dt)

        shift_col <- shift_col_finder(row_dt, n_try)
        resolved_col <- fix_one_row(rn, shift_col, n_try)

        fixed_row <- dt[.(rn)]
        log_fix(
          rn,
          n_try,
          resolved_col,
          "hidden",
          "fixed",
          orig_v,
          orig_s,
          find_violations(fixed_row),
          snapshot_row(fixed_row)
        )
      }
      suspect_rows <- setdiff(suspect_rows, hidden_rows)
      already_fixed <- c(already_fixed, hidden_rows)
    }
  }

  # --- Cleanup ---
  if (length(rows_to_drop) > 0L) {
    message(sprintf(
      "auto_fix_tab_shifts: dropped %d unfixable rows",
      length(rows_to_drop)
    ))
    dt <- dt[!n %in% rows_to_drop]
  }

  fixes <- if (length(fix_log) > 0L) {
    do.call(rbind, fix_log)
  } else {
    data.frame(
      row_n = integer(),
      n_extra = integer(),
      shift_col = character(),
      pass = character(),
      status = character(),
      original_violations = character(),
      original_vals = character(),
      fixed_violations = character(),
      fixed_vals = character(),
      stringsAsFactors = FALSE
    )
  }

  n_fixed <- sum(fixes$status == "fixed")
  n_dropped <- sum(fixes$status == "dropped")
  message(sprintf(
    "auto_fix_tab_shifts summary: %d fixed, %d dropped",
    n_fixed,
    n_dropped
  ))

  # --- Validation ---
  n_total_touched <- length(unique(fixes$row_n))
  if (n_total_touched > 150L) {
    stop(sprintf(
      "auto_fix_tab_shifts validation FAILED: %d rows edited, exceeds 100-row safety limit",
      n_total_touched
    ))
  }

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
      bad_ns <- fixed_fixes$row_n[still_bad]
      message(sprintf(
        paste0(
          "auto_fix_tab_shifts: dropping %d rows with unresolvable multi-point ",
          "tab shifts: rows %s\n  Details: %s"
        ),
        sum(still_bad),
        paste(head(bad_ns, 10), collapse = ", "),
        paste(head(fixed_fixes$fixed_violations[still_bad], 5), collapse = "; ")
      ))
      fixes$status[fixes$row_n %in% bad_ns] <- "dropped_after_fix"
      dt <- dt[!n %in% bad_ns]
      n_dropped <- n_dropped + sum(still_bad)
    }

    message(sprintf(
      "auto_fix_tab_shifts validation: %d fixes verified, %d unfixable dropped",
      sum(fixes$status == "fixed"),
      sum(still_bad)
    ))
  }

  # Row count check (after all drops are finalized)
  final_nrow <- nrow(dt)
  expected_nrow <- initial_nrow - n_dropped
  if (final_nrow != expected_nrow) {
    stop(sprintf(
      "auto_fix_tab_shifts validation FAILED: row count — started %d, dropped %d, expected %d, got %d",
      initial_nrow,
      n_dropped,
      expected_nrow,
      final_nrow
    ))
  }
  message(sprintf(
    "auto_fix_tab_shifts validation: row count OK (%d - %d dropped = %d)",
    initial_nrow,
    n_dropped,
    final_nrow
  ))

  if (length(sample_ns) > 0L) {
    all_touched <- unique(fixes$row_n)
    keep_mask <- !sample_ns %in% all_touched
    if (any(keep_mask)) {
      verify_ns <- sample_ns[keep_mask]
      verify_snapshot <- untouched_snapshot[keep_mask, ]
      if (n_dropped > 0L) {
        dropped_ns <- fixes$row_n[
          fixes$status %in% c("dropped", "dropped_after_fix")
        ]
        kept <- setdiff(seq_len(initial_nrow), dropped_ns)
        verify_fixed_pos <- match(verify_ns, kept)
      } else {
        verify_fixed_pos <- verify_ns
      }
      current <- dt[verify_fixed_pos, ..check_cols]
      diffs <- !mapply(
        function(a, b) identical(as.character(a), as.character(b)),
        verify_snapshot,
        current
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

  dt[, n := NULL]
  list(dt = dt, fixes = fixes)
}
