# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbQuoteLiteral
#' @usage NULL
.dbQuoteLiteral <- function(conn, x, ...) {
  if (methods::is(x, "SQL")) {
    return(x)
  }

  if (is.factor(x)) {
    return(DBI::dbQuoteString(conn, as.character(x)))
  }

  if (is.character(x)) {
    return(DBI::dbQuoteString(conn, x))
  }

  # This is necessary because a simple double literal is interpreted as DECIMAL
  if (is.numeric(x) & typeof(x) == "double") {
    x_str <- as.character(x)
    x_str[is.na(x_str)] <- "NULL"
    return(DBI::SQL(paste0("CAST(", x_str, " AS DOUBLE)")))
  }

  if (inherits(x, "POSIXct")) {
    tz <- attr(x, "tzone")
    if (is.null(tz) || tz == "") {
      tz <- Sys.timezone()
    }
    ts <- strftime(x, "%Y-%m-%d %H:%M:%S", tz = conn@session.timezone)
    ts_str <- DBI::dbQuoteString(conn, ts)
    is_ts_null <- ts_str == DBI::SQL("NULL")
    ts_str[!is_ts_null] <- DBI::SQL(paste0("TIMESTAMP ", ts_str[!is_ts_null]))
    return(ts_str)
  }

  if (inherits(x, "Date")) {
    ds <- DBI::dbQuoteString(conn, as.character(x))
    is_ds_null <- ds == DBI::SQL("NULL")
    ds[!is_ds_null] <- DBI::SQL(paste0("DATE ", ds[!is_ds_null]))
    return(ds)
  }

  if (inherits(x, "difftime")) {
    format_hms <- utils::getFromNamespace("format_hms", "hms")
    hms <- format_hms(x)
    hms_str <- DBI::dbQuoteString(conn, hms)
    is_hms_null <- hms_str == DBI::SQL("NULL")
    hms_str[!is_hms_null] <- DBI::SQL(paste0("TIME ", hms_str[!is_hms_null]))
    return(hms_str)
  }

  if (is.list(x)) {
    blob_data <- vapply(
      x,
      function(x) {
        if (length(x) == 1L && (is.null(x) || is.na(x))) {
          "NULL"
        } else if (is.raw(x)) {
          if (rawToChar(x) == "NA") {
            "NULL"
          } else {
            paste0("X'", paste(format(x), collapse = ""), "'")
          }
        } else {
          if (!is.null(names(x))) {
            stop("MAP values are not supported.", call. = FALSE)
          }
          inner_values <- .dbQuoteLiteral(conn, x, ...)
          paste0("ARRAY[", paste(inner_values, collapse = ", "), "]")
        }
      },
      character(1)
    )
    return(DBI::SQL(blob_data, names = names(x)))
  }

  if (is.logical(x)) {
    x <- tolower(as.character(x))
  }

  x <- as.character(x)
  x[is.na(x)] <- "NULL"
  DBI::SQL(x, names = names(x))
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbQuoteLiteral
#' @export
setMethod("dbQuoteLiteral", signature("PrestoConnection"), .dbQuoteLiteral)
