# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Append to a table in database using a statement
#'
#' @inheritParams sqlAppendTableAs
#' @param conn a `PrestoConnection` object, as returned by [DBI::dbConnect()].
#' @param sql a character string containing SQL statement, or a `tbl_presto` object.
#' @export
setGeneric("dbAppendTableAs",
  def = function(conn, name, sql, auto_reorder = TRUE, ...) {
    standardGeneric("dbAppendTableAs")
  }
)

#' @rdname PrestoConnection-class
#' @usage NULL
.dbAppendTableAs <- function(conn, name, sql, auto_reorder = TRUE, ...) {
  # Generate SQL with validation and reordering (handled in sqlAppendTableAs)
  query <- sqlAppendTableAs(
    con = conn,
    name = name,
    sql = sql,
    auto_reorder = auto_reorder,
    ...
  )
  
  rows_inserted <- DBI::dbExecute(conn, query)
  return(rows_inserted)
}

#' @rdname PrestoConnection-class
#' @export
setMethod(
  "dbAppendTableAs", signature("PrestoConnection"),
  .dbAppendTableAs
)

