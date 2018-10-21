# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbplyr_compatible.R PrestoConnection.R
NULL

#' S3 implementation of \code{\link[dplyr]{db_explain}} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dplyr::db_explain,PrestoConnection)
#' } else {
#'   export(db_explain.PrestoConnection)
#' }
db_explain.PrestoConnection <- function(con, sql, ...) {
  build_sql <- dbplyr_compatible('build_sql')
  explain.sql <- build_sql("EXPLAIN ", sql)
  explanation <- DBI::dbGetQuery(con, explain.sql)
  return(paste(explanation[[1]], collapse = "\n"))
}
