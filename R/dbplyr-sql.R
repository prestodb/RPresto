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
sql_query_save.PrestoConnection <- function(
  con, sql, name, temporary = TRUE, ..., with = NULL
) {
  if (!identical(temporary, FALSE)) {
    stop(
      'Temporary table is not supported in Presto. ',
      'Use temporary = FALSE to save the query in a permanent table.',
      call. = FALSE
    )
  }
  sqlCreateTableAs(con, name, sql, with, ...)
}
