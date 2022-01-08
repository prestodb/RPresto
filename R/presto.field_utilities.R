# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

print.presto.field <- function(prf, prefix = "", ...) {
  cat(prefix, "presto.field: \n", sep = "")
  cat(prefix, "..Name: ", prf$name_, "\n", sep = "")
  cat(prefix, "..Type:  ", prf$type_, "\n", sep = "")
  if (prf$is_map_) {
    cat(prefix, "..Key Type:  ", prf$key_type_, "\n", sep = "")
  }
  cat(prefix, "..Is Array:  ", prf$is_array_, "\n", sep = "")
  cat(prefix, "..Is Map:  ", prf$is_map_, "\n", sep = "")
  cat(prefix, "..Is Row:  ", prf$is_row_, "\n", sep = "")
  cat(prefix, "..Is parent Map:  ", prf$is_parent_map_, "\n", sep = "")
  cat(prefix, "..Is parent Array:  ", prf$is_parent_array_, "\n", sep = "")
  cat(prefix, "..Fields:  ", length(prf$fields_), " fields\n", sep = "")
  if (!is.na(prf$timezone_)) {
    cat(prefix, "..Timezone:  ", prf$timezone_, "\n", sep = "")
  }
  if (length(prf$fields_) > 0) {
    for (f in prf$fields_) {
      print(f, prefix = paste0(prefix, "...."))
    }
  }
  invisible(prf)
}

is_simple_type <- function(prf) {
  return((!prf$type_ %in% c("PRESTO_ROW", "PRESTO_MAP")))
}

parse.timestamp <- function(x, timezone) {
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("The [", x$name_, "] field is a TIMESTAMP WITH TIMEZONE ",
         "field. Please install the lubridate package or cast the field to ",
         "VARCHAR in Presto before importing it to R.",
         call. = FALSE)
  }
  if (is.na(x)) {
    return(as.POSIXct(x, tz = timezone))
  }
  # A time stamp with timezone has 3 parts separated by a spcae
  # e.g. 2000-01-01 00:01:02 America/Los_Angeles
  # The first part is the date; the second the time in the day, and the third
  # the timezone
  parts <- unlist(strsplit(x, " "))
  if (is.na(parts[3])) {
    warning("No timezone detected in timestamp ", x, call. = FALSE)
  }
  parsed_ts_with_tz <- lubridate::ymd_hms(
    paste(parts[1:2], collapse = " "),
    tz = ifelse(is.na(parts[3]), "", parts[3])
  )
  return(lubridate::with_tz(parsed_ts_with_tz, tz = timezone))
}

parse.time_with_tz <- function(x, timezone) {
  if (!requireNamespace("hms", quietly = TRUE)) {
    stop("The [", x$name_, "] field is a TIME WITH TIME ZONE ",
         "field. Please install the hms package or cast the field to VARCHAR ",
         "before importing it to R.",
         call. = FALSE)
  }
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("The [", x$name_, "] field is a TIMESTAMP WITH TIMEZONE ",
         "field. Please install the lubridate package or cast the field to ",
         "VARCHAR before importing it to R.",
         call. = FALSE)
  }
  if (is.na(x)) {
    return(hms::as_hms(as.POSIXct(x, tz = timezone)))
  }
  placeholder_date <- "2000-01-01"
  datetime_with_tz <- parse.timestamp(
    paste(placeholder_date, x, sep = " "),
    timezone = timezone
  )
  time_with_tz <- hms::as_hms(datetime_with_tz)
  return(time_with_tz)
}

parse.interval_year_to_month <- function(x) {
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("The [", x$name_, "] field is an INTERVAL YEAR TO MONTH ",
         "field. Please install the lubridate package or cast the field to ",
         "other types before importing it to R.",
         call. = FALSE)
  }
  if (is.na(x)) {
    return(x)
  }
  year_month <- as.integer(unlist(strsplit(x, "-")))
  d <- lubridate::duration(year_month[1], units = "years") +
    lubridate::duration(year_month[2], units = "months")
  return(d)
}

parse.interval_day_to_second <- function(x) {
  if (!requireNamespace("lubridate", quietly = TRUE)) {
    stop("The [", x$name_, "] field is an INTERVAL YEAR TO MONTH ",
         "field. Please install the lubridate package or cast the field to ",
         "other types before importing it to R.",
         call. = FALSE)
  }
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
    lubridate::duration(day_to_millisecond[5]/1000, units = "seconds")
  return(d)
}

unlist_with_attr <- function(x) {
  nms <- names(x)
  # We use a do.call("c", ...) hack to "unlist" the list as a naive
  # call of unlist() would strip the datetime attributes.
  res <- do.call(base::c, unname(x))
  names(res) <- nms
  return(res)
}

replace_null_with_na <- function(x) {
  if (is.list(x)) {
    purrr::modify(x, ~. %||% NA)
  } else {
    x %||% NA
  }
}

extract.schema.attr <- function(prf) {
  tibble::tibble(
    name = purrr::map_chr(prf, ~.$name_),
    is_array = purrr::map_lgl(prf, ~.$is_array_),
    is_map = purrr::map_lgl(prf, ~.$is_map_),
    is_row = purrr::map_lgl(prf, ~.$is_row_)
  )
}

# This function is adapted from
# https://github.com/r-dbi/bigrquery/blob/main/R/bq-download.R
convert_bigint <- function(df, bigint) {
  if (bigint == "integer64") {
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