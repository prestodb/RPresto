# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @param fail_if_missing If `FALSE`, `dbRemoveTable()` succeeds if the
#'   table doesn't exist.
#' @usage NULL
.dbRemoveTable <- function(conn, name, ..., fail_if_missing = TRUE) {
  name <- DBI::dbQuoteIdentifier(conn, name)
  if (fail_if_missing) {
    extra <- ""
  } else {
    extra <- "IF EXISTS"
  }
  DBI::dbExecute(conn, paste("DROP TABLE", extra, name))
  invisible(TRUE)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbRemoveTable
#' @export
setMethod("dbRemoveTable", signature("PrestoConnection"), .dbRemoveTable)
