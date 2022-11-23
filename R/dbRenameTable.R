# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Rename a table
#'
#' @param conn A PrestoConnection.
#' @param name Existing table's name.
#' @param new_name New table name.
#' @param ... Extra arguments passed to dbExecute.
#' @export
setGeneric("dbRenameTable",
  def = function(conn, name, new_name, ...) {
    standardGeneric("dbRenameTable")
  }
)

#' @rdname PrestoConnection-class
#' @usage NULL
.dbRenameTable <- function(conn, name, new_name, ...) {
  if (!DBI::dbExistsTable(conn, name)) {
    stop("Table ", as.character(name), " doesn't exist.", call. = FALSE)
  }
  name <- DBI::dbQuoteIdentifier(conn, name)
  new_name <- DBI::dbQuoteIdentifier(conn, new_name)
  sql <- DBI::SQL(paste0("ALTER TABLE ", name, " RENAME TO ", new_name))
  DBI::dbExecute(conn, sql, ...)
}

#' @rdname PrestoConnection-class
#' @export
setMethod(
  "dbRenameTable", signature("PrestoConnection"),
  .dbRenameTable
)
