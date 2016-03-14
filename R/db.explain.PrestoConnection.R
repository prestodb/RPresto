# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' S3 implementation of \code{\link[dplyr]{db_explain}} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#' @export
db_explain.PrestoConnection <- function(con, sql, ...) {
  explain.sql <- dplyr::build_sql("EXPLAIN ", sql)
  explanation <- DBI::dbGetQuery(con, explain.sql)
  return(paste(explanation[[1]], collapse = "\n"))
}
