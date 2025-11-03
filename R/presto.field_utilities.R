# Package-level default cache for bigint overflow warnings
.rpresto_default_warned_env <- new.env(parent = emptyenv())

# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @export
print.presto.field <- function(x, ..., prefix = "") {
  cat(prefix, "presto.field: \n", sep = "")
  cat(prefix, "..Name: ", x$name_, "\n", sep = "")
  cat(prefix, "..Type:  ", x$type_, "\n", sep = "")
  if (x$is_map_) {
    cat(prefix, "..Key Type:  ", x$key_type_, "\n", sep = "")
  }
  cat(prefix, "..Is Array:  ", x$is_array_, "\n", sep = "")
  cat(prefix, "..Is Map:  ", x$is_map_, "\n", sep = "")
  cat(prefix, "..Is Row:  ", x$is_row_, "\n", sep = "")
  cat(prefix, "..Is parent Map:  ", x$is_parent_map_, "\n", sep = "")
  cat(prefix, "..Is parent Array:  ", x$is_parent_array_, "\n", sep = "")
  cat(prefix, "..Fields:  ", length(x$fields_), " fields\n", sep = "")
  if (!is.na(x$session_timezone_)) {
    cat(prefix, "..Session time zone:  ", x$session_timezone_, "\n", sep = "")
  }
  if (!is.na(x$output_timezone_)) {
    cat(prefix, "..Output time zone:  ", x$output_timezone_, "\n", sep = "")
  }
  if (length(x$fields_) > 0) {
    for (f in x$fields_) {
      print(f, prefix = paste0(prefix, "...."))
    }
  }
  invisible(x)
}

is_simple_type <- function(prf) {
  return((!prf$type_ %in% c("PRESTO_ROW", "PRESTO_MAP")))
}

parse.timestamp_with_tz <- function(x, output_timezone) {
  if (is.na(x)) {
    return(as.POSIXct(x, tz = output_timezone))
  }
  # A time stamp with timezone has 3 parts separated by a spcae
  # e.g. 2000-01-01 00:01:02 America/Los_Angeles
  # The first part is the date; the second the time in the day, and the third
  # the timezone
  parts <- unlist(strsplit(x, " "))
  if (is.na(parts[3])) {
    warning("No timezone detected in timestamp ", x, call. = FALSE)
  }
  # Trino passes timezone in the UTC offset format, e.g. +08:00
  if (stringi::stri_detect_regex(parts[3], "[\\+\\-]\\d\\d:\\d\\d")) {
    parsed_ts_in_utc <- lubridate::ymd_hms(
      paste(parts[1:2], collapse = " "), tz = "UTC"
    )
    parsed_tz <- stringi::stri_match_all_regex(
      parts[3], "([\\+\\-])(\\d\\d):(\\d\\d)"
    )[[1]][1, ]
    if (length(parsed_tz) != 4) {
      stop("The timezone ", parts[3], " cannot be parsed.", call. = FALSE)
    }
    utc_offset_sign <- ifelse(parsed_tz[2] == "+", -1, 1)
    utc_offset_seconds <-
      as.numeric(parsed_tz[3]) * 60 * 60 + as.numeric(parsed_tz[4]) * 60
    parsed_ts_with_tz <- parsed_ts_in_utc + utc_offset_sign * utc_offset_seconds
  } else {
    parsed_ts_with_tz <- lubridate::ymd_hms(
      paste(parts[1:2], collapse = " "),
      tz = ifelse(is.na(parts[3]), "", parts[3])
    )
  }
  return(lubridate::with_tz(parsed_ts_with_tz, tz = output_timezone))
}

parse.time_with_tz <- function(x, output_timezone, timestamp) {
  if (!requireNamespace("hms", quietly = TRUE)) {
    stop("The [", x$name_, "] field is a TIME WITH TIME ZONE ",
      "field. Please install the hms package or cast the field to VARCHAR ",
      "before importing it to R.",
      call. = FALSE
    )
  }
  if (is.na(x)) {
    return(hms::as_hms(as.POSIXct(x, tz = output_timezone)))
  }
  # Trino TIME WITH TIME ZONE type uses a different format
  if (stringi::stri_detect_regex(x, ".+[\\+\\-]\\d\\d:\\d\\d")) {
    x <- stringi::stri_replace_first_regex(x, "([\\+\\-])", " $1")
  }
  # Use query execution date as the placeholder date
  placeholder_date <- as.character(as.Date(timestamp))
  datetime_with_tz <- parse.timestamp_with_tz(
    paste(placeholder_date, x, sep = " "),
    output_timezone = output_timezone
  )
  time_with_tz <- hms::as_hms(datetime_with_tz)
  return(time_with_tz)
}

parse.interval_year_to_month <- function(x) {
  if (is.na(x)) {
    return(x)
  }
  year_month <- as.integer(unlist(strsplit(x, "-")))
  d <- lubridate::duration(year_month[1], units = "years") +
    lubridate::duration(year_month[2], units = "months")
  return(d)
}

parse.interval_day_to_second <- function(x) {
  if (is.na(x)) {
    return(x)
  }
  day_subday <- unlist(strsplit(x, " "))
  second_millisecond <- unlist(strsplit(day_subday[2], "\\."))
  hour_minute_second <- unlist(strsplit(second_millisecond[1], ":"))
  day_to_millisecond <- as.integer(
    c(day_subday[1], hour_minute_second, second_millisecond[2])
  )
  d <- lubridate::duration(day_to_millisecond[1], units = "days") +
    lubridate::duration(day_to_millisecond[2], units = "hours") +
    lubridate::duration(day_to_millisecond[3], units = "minutes") +
    lubridate::duration(day_to_millisecond[4], units = "seconds") +
    lubridate::duration(day_to_millisecond[5] / 1000, units = "seconds")
  return(d)
}

unlist_with_attr <- function(x) {
  nms <- names(x)
  # We use a do.call("c", ...) hack to "unlist" the list as a naive
  # call of unlist() would strip the datetime attributes.
  res <- do.call(base::c, unname(x))
  if (length(x) > 0L) {
    names(res) <- nms
  }
  return(res)
}

replace_null_with_na <- function(x) {
  if (is.list(x)) {
    purrr::modify(x, ~ . %||% NA)
  } else {
    x %||% NA
  }
}

extract.schema.attr <- function(prf) {
  tibble::tibble(
    name = purrr::map_chr(prf, ~ .$name_),
    is_array = purrr::map_lgl(prf, ~ .$is_array_),
    is_map = purrr::map_lgl(prf, ~ .$is_map_),
    is_row = purrr::map_lgl(prf, ~ .$is_row_)
  )
}

# This function is adapted from
# https://github.com/r-dbi/bigrquery/blob/main/R/bq-download.R
convert_bigint <- function(df, bigint) {
  if (bigint == "integer64") {
    return(df)
  }

  if (bigint == "integer") {
    # Use a per-query cache if provided via option; otherwise fallback
    warned <- getOption(
      "rpresto.bigint_overflow.warned_env",
      .rpresto_default_warned_env
    )
    # Column-aware overflow warnings for top-level integer64 columns
    nms <- names(df)
    if (!is.null(nms)) {
      for (col_name in nms) {
        col <- df[[col_name]]
        if (bit64::is.integer64(col)) {
          df[[col_name]] <- coerce_integer64_to_int_with_warning(
            col, col_name, warned
          )
        } else {
          # Recurse into nested structures while preserving the
          # top-level column name
          df[[col_name]] <- rapply_int64_with_colname(col, col_name, warned)
        }
      }
    }
    return(df)
  }

  as_bigint <- switch(bigint,
    integer = as.integer,
    numeric = as.numeric,
    character = as.character
  )

  rapply_int64(df, f = as_bigint)
}

rapply_int64 <- function(x, f) {
  if (is.list(x)) {
    x[] <- lapply(x, rapply_int64, f = f)
    x
  } else if (bit64::is.integer64(x)) {
    f(x)
  } else {
    x
  }
}

# Recursively apply integer64->integer conversion while preserving top-level column name for warnings
rapply_int64_with_colname <- function(x, col_name, warned) {
  if (is.list(x)) {
    # Recurse element-wise, appending child names when available
    # (e.g., data.frame columns)
    nms <- names(x)
    for (i in seq_along(x)) {
      child_is_named <- !is.null(nms) && !is.na(nms[i]) && nzchar(nms[i])
      child_name <- if (child_is_named) nms[i] else ""
      child_path <- if (nzchar(child_name)) {
        paste0(col_name, ".", child_name)
      } else {
        col_name
      }
      x[[i]] <- rapply_int64_with_colname(x[[i]], child_path, warned)
    }
    return(x)
  } else if (bit64::is.integer64(x)) {
    return(coerce_integer64_to_int_with_warning(x, col_name, warned))
  } else {
    return(x)
  }
}

# Internal helper: convert integer64 to integer with column-aware overflow warning
coerce_integer64_to_int_with_warning <- function(x, col_name, warned = NULL) {
  warn_enabled <- isTRUE(
    getOption("rpresto.bigint_overflow.warning", TRUE)
  )

  if (!warn_enabled) {
    return(suppressWarnings(as.integer(x)))
  }

  INT_MIN64 <- bit64::as.integer64("-2147483648")
  INT_MAX64 <- bit64::as.integer64("2147483647")

  is_na <- bit64::is.na.integer64(x)
  oob <- (x < INT_MIN64 | x > INT_MAX64) & !is_na
  n_oob <- sum(oob, na.rm = TRUE)

  if (n_oob > 0L) {
    # Suppress duplicate warnings for the same column path within one coercion
    if (!is.null(warned)) {
      already <- isTRUE(get0(col_name, envir = warned, inherits = FALSE))
      if (already) {
        return(suppressWarnings(as.integer(x)))
      }
      assign(col_name, TRUE, envir = warned)
    }
    msg <- sprintf(
      "BIGINT to integer overflow in column '%s' (n=%d); coerced to NA",
      col_name,
      n_oob
    )
    warning(msg, call. = FALSE)
  }

  return(suppressWarnings(as.integer(x)))
}
