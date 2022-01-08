# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' S3 implementation of \code{db_collect} for Presto.
#'
#' @importFrom dbplyr db_collect
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_collect.PrestoConnection <- function(
  con, sql, n = -1, warn_incomplete = TRUE, ...
) {
  res <- dbSendQuery(con, sql, ...)
  tryCatch({
      out <- dbFetch(res, n = n)
  }, finally = {
      dbClearResult(res)
  })
  out
}
