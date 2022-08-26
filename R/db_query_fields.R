# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' S3 implementation of \code{db_query_fields} for Presto.
#'
#' @importFrom dplyr db_query_fields
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_query_fields.PrestoConnection <- function(con, sql, ...) {
  fields <- sql_query_fields(con, sql, ...)
  df <- dbGetQuery(con, fields)
  return(colnames(df))
}
