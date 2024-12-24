# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

iris.sql <- function() {
  .row.to.select <- function(...) {
    items <- list(...)
    types <- lapply(items, data.class)
    presto.types <- ifelse(unlist(types) == "numeric", "DOUBLE", "VARCHAR")
    values <- unlist(lapply(items, as.character))

    columns <- paste("CAST('", values, "' AS ", presto.types, ') AS "', names(items), '"', sep = "", collapse = ", ")
    return(paste("SELECT", columns))
  }
  rows <- do.call("mapply", c(FUN = .row.to.select, as.list(iris)))

  sql <- paste("(", paste(rows, collapse = " UNION ALL "), ") iris")

  return(sql)
}

iris_df <- dplyr::rename_with(iris, tolower)

test_df <- tibble::tibble(
  field1 = c("a", "b", NA_character_),
  field2 = c(1L, 2L, NA_integer_),
  field3 = c(3.14, 2.72, NA_real_),
  field4 = c(TRUE, FALSE, NA),
  field5 = as.Date(c("2000-01-01", "2000-01-01", NA_character_)),
  field6 = c(
    lubridate::ymd_hms("2000-01-01 01:02:03", tz = "America/Los_Angeles"),
    lubridate::ymd_hms("2000-01-02 04:05:06", tz = "America/Los_Angeles"),
    as.POSIXct(NA)
  ),
  field7 = hms::as_hms(c("01:02:03", "04:05:06", NA_character_)),
  field8 = list(c(1L, 2L), c(3L, NA_integer_), NA_integer_),
  field9 = list(c(1.1, 2.2), c(3.3, NA_real_), NA_real_)
)

# Note that you need to wrap your test_that call with with_locale if you
# use the data returned here for comparison
data.frame.with.all.classes <- function(row.indices) {
  old.locale <- Sys.getlocale("LC_CTYPE")
  Sys.setlocale("LC_CTYPE", test.locale())
  on.exit(Sys.setlocale("LC_CTYPE", old.locale), add = TRUE)

  e <- tibble::tibble(
    logical = c(TRUE, FALSE),
    integer = c(1L, 2L),
    numeric = c(0.0, 1.0),
    character1 = c("0", "1.414"),
    character2 = c("", "z"),
    raw = list(raw(0), raw(0)),
    Date = as.Date(c("2015-03-01", "2015-03-02")),
    POSIXct = as.POSIXct(
      c("2015-03-01 12:00:00", "2015-03-02 12:00:00.321"),
      tz = test.timezone()
    ),
    # The first element is 'ıİÖğ' in iso8859-9 encoding,
    # and the second 'Face with tears of joy' in UTF-8
    character3 = c("\xFD\xDD\xD6\xF0", "\u1F602"),
    list_unnamed = list(list(1, 2), list()),
    list_named = list(list(a = 1, b = 2), list())
  )
  if (!missing(row.indices)) {
    rv <- e[row.indices, , drop = FALSE]
    return(rv)
  }
  return(e)
}
