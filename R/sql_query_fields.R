# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' S3 implementation of \code{sql_query_fields} for Presto.
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
