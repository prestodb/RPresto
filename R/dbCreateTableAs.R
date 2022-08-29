# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Create a table in database using a statement
#'
#' @inheritParams DBI::dbReadTable
#' @inheritParams sqlCreateTableAs
#' @export
setGeneric("dbCreateTableAs",
  def = function(conn, name, statement, with = NULL, ...) {
    standardGeneric("dbCreateTableAs")
  }
)

#' @rdname PrestoConnection-class
#' @param statement a character string containing SQL.
#' @usage NULL
.dbCreateTableAs <- function(conn, name, statement, with = NULL, ...) {
  stopifnot(is.character(statement), length(statement) == 1)

  query <- sqlCreateTableAs(
    con = conn,
    table = name,
    statement = statement,
    with = with,
    ...
  )
  DBI::dbExecute(conn, query)
  invisible(TRUE)
}

#' @rdname PrestoConnection-class
#' @export
setMethod(
  'dbCreateTableAs', signature('PrestoConnection'),
  .dbCreateTableAs
)
