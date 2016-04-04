# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' S3 implementation of \code{sql_translate_env} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#' @export
db_query_fields.PrestoConnection <- function(con, sql, ...) {
  fields <- dplyr::build_sql(
    "SELECT * FROM ", sql, " LIMIT 0",
    con = con
  )
  result <- dbSendQuery(con, fields)
  on.exit(dbClearResult(result))
  return(dbListFields(result))
}
