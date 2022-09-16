# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' dbplyr SQL methods
#'
#' @rdname dbplyr-sql
#' @importFrom dbplyr sql_query_save
#' @inheritParams sqlCreateTableAs
#' @export
#' @param temporary If a temporary table should be created. Default to TRUE in
#'   the [dbplyr::sql_query_save()] generic. The default value generates an
#'   error in Presto. Using `temporary = FALSE` to save the query in a
#'   permanent table.
#' @md
sql_query_save.PrestoConnection <- function(con, sql, name, temporary = TRUE, ..., with = NULL) {
  if (!identical(temporary, FALSE)) {
    stop(
      "Temporary table is not supported in Presto. ",
      "Use temporary = FALSE to save the query in a permanent table.",
      call. = FALSE
    )
  }
  sqlCreateTableAs(con, name, sql, with, ...)
}

#' S3 implementation of `sql_query_fields` for Presto.
#'
#' @importFrom dbplyr sql_query_fields
#' @export
#' @rdname dbplyr_function_implementations
#' @keywords internal
sql_query_fields.PrestoConnection <- function(con, sql, ...) {
  dbplyr::build_sql(
    "SELECT * FROM ", dplyr::sql_subquery(con, sql), " WHERE 1 = 0",
    con = con
  )
}

#' S3 implementation of custom escape method for [sql_escape_date][dbplyr::sql_escape_date]
#'
#' @importFrom dbplyr sql_escape_date
#' @export
#' @rdname dbplyr_function_implementations
#' @keywords internal
sql_escape_date.PrestoConnection <- function(con, x) {
  paste0("DATE ", DBI::dbQuoteString(con, as.character(x)))
}

#' S3 implementation of custom escape method for [sql_escape_datetime][dbplyr::sql_escape_datetime]
#'
#' @importFrom dbplyr sql_escape_datetime
#' @export
#' @rdname dbplyr_function_implementations
#' @keywords internal
sql_escape_datetime.PrestoConnection <- function(con, x) {
  # Use unix time to minimize reliance on time zone particulars.
  paste0("FROM_UNIXTIME(", as.numeric(x), ")")
}

presto_window_functions <- function() {
  return(dbplyr::sql_translator(
    .parent = dbplyr::base_win,
    all = dbplyr::win_recycled("bool_and"),
    any = dbplyr::win_recycled("bool_or"),
    n_distinct = dbplyr::win_absent("n_distinct"),
    sd = dbplyr::win_recycled("stddev_samp"),
    quantile = function(...) stop(quantile_error_message(), call. = FALSE),
    median = function(...) stop(quantile_error_message("median"), call. = FALSE)
  ))
}

#' Create error messages for quantile-like functions
#'
#' @param f a string giving the name of the function
#'
#' @return error message for `f`
#' @keywords internal
#' @noRd
quantile_error_message <- function(f = "quantile") {
  paste(
    paste0("`", f, "()`"),
    "is not supported in this SQL variant,",
    "try `approx_percentile()` instead; see Presto documentation."
  )
}

#' S3 implementation of `sql_translation` for Presto.
#'
#' @importFrom dbplyr sql_translation
#' @export
#' @rdname dbplyr_function_implementations
#' @keywords internal
sql_translation.PrestoConnection <- function(con) {
  return(dbplyr::sql_variant(
    dbplyr::sql_translator(
      .parent = dbplyr::base_scalar,
      ifelse = dbplyr::sql_prefix("IF"),
      as = function(column, type) {
        sql_type <- stringi::stri_trans_toupper(
          dbDataType(Presto(), type),
          "en_US.UTF-8"
        )
        dbplyr::build_sql("CAST(", column, " AS ", dbplyr::sql(sql_type), ")")
      },
      as.character = dbplyr::sql_cast("VARCHAR"),
      as.numeric = dbplyr::sql_cast("DOUBLE"),
      as.double = dbplyr::sql_cast("DOUBLE"),
      as.integer = dbplyr::sql_cast("BIGINT"),
      as.Date = dbplyr::sql_cast("DATE"),
      as.logical = dbplyr::sql_cast("BOOLEAN"),
      as.raw = dbplyr::sql_cast("VARBINARY"),
      tolower = dbplyr::sql_prefix("LOWER"),
      toupper = dbplyr::sql_prefix("UPPER"),
      pmax = dbplyr::sql_prefix("GREATEST"),
      pmin = dbplyr::sql_prefix("LEAST"),
      is.finite = dbplyr::sql_prefix("IS_FINITE"),
      is.infinite = dbplyr::sql_prefix("IS_INFINITE"),
      is.nan = dbplyr::sql_prefix("IS_NAN"),
      `[[` = function(x, i) {
        if (is.numeric(i) && isTRUE(all.equal(i, as.integer(i)))) {
          i <- as.integer(i)
        }
        dbplyr::build_sql("ELEMENT_AT(", x, ", ", i, ")")
      },
      quantile = function(...) stop(quantile_error_message(), call. = FALSE),
      median = function(...) stop(quantile_error_message("median"), call. = FALSE)
    ),
    dbplyr::sql_translator(
      .parent = dbplyr::base_agg,
      n = function() dbplyr::sql("COUNT(*)"),
      sd = dbplyr::sql_prefix("STDDEV_SAMP"),
      var = dbplyr::sql_prefix("VAR_SAMP"),
      all = dbplyr::sql_prefix("BOOL_AND"),
      any = dbplyr::sql_prefix("BOOL_OR"),
      quantile = function(...) stop(quantile_error_message(), call. = FALSE),
      median = function(...) stop(quantile_error_message("median"), call. = FALSE)
    ),
    presto_window_functions()
  ))
}
