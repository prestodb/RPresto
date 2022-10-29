# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbAppendTable
#' @usage NULL
.dbAppendTable <- function(conn, name, value, ...) {
  is_factor <- vapply(value, is.factor, logical(1L))
  if (any(is_factor)) {
    value[is_factor] <- lapply(value[is_factor], as.character)
  }
  sql <- DBI::sqlAppendTable(conn, name, value, row.names = FALSE)
  DBI::dbExecute(conn, sql)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbAppendTable
#' @export
setMethod(
  "dbAppendTable",
  c("PrestoConnection", "character", "data.frame"),
  .dbAppendTable
)
