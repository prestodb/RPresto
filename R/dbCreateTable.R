# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbCreateTable
#' @param conn a `PrestoConnection` object, as returned by [DBI::dbConnect()].
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' @usage NULL
.dbCreateTable <- function(conn, name, fields, with = NULL, ..., row.names = NULL, temporary = FALSE) {
  stopifnot(is.null(row.names))
  if (!isFALSE(temporary)) {
    stop("CREATE TEMPORARY TABLE is not supported in Presto.", call. = FALSE)
  }

  query <- sqlCreateTable(
    con = conn,
    table = name,
    fields = fields,
    row.names = row.names,
    temporary = temporary,
    with = with,
    ...
  )
  DBI::dbExecute(conn, query)
  invisible(TRUE)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbCreateTable
#' @export
setMethod("dbCreateTable", signature("PrestoConnection"), .dbCreateTable)
