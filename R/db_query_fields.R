# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbplyr_compatible.R
NULL

#' S3 implementation of \code{db_query_fields} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#' @export
db_query_fields.PrestoConnection <- function(con, sql, ...) {
  build_sql <- dbplyr_compatible('build_sql')
  fields <- build_sql(
    "SELECT * FROM ", dplyr::sql_subquery(con, sql), " WHERE 1 = 0",
    con = con
  )
  df <- dbGetQuery(con, fields)
  return(colnames(df))
}
