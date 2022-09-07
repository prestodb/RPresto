# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Compose query to create a simple table using a statement
#'
#' @param con A database connection.
#' @param table The table name, passed on to [dbQuoteIdentifier()]. Options are:
#'   - a character string with the unquoted DBMS table name,
#'     e.g. `"table_name"`,
#'   - a call to [Id()] with components to the fully qualified table name,
#'     e.g. `Id(schema = "my_schema", table = "table_name")`
#'   - a call to [SQL()] with the quoted and fully qualified table name
#'     given verbatim, e.g. `SQL('"my_schema"."table_name"')`
#' @param statement a character string containing SQL.
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' @param ... Other arguments used by individual methods.
#' @export
setGeneric("sqlCreateTableAs",
  def = function(con, table, statement, with = NULL, ...) {
    standardGeneric("sqlCreateTableAs")
  }
)

#' @rdname PrestoConnection-class
#' @usage NULL
.sqlCreateTableAs <- function(con, table, statement, with = NULL, ...) {
    table <- DBI::dbQuoteIdentifier(con, table)

    DBI::SQL(paste0(
      'CREATE TABLE ', table, ' AS\n', statement, '\n', with
    ))
}

#' @rdname PrestoConnection-class
#' @export
setMethod("sqlCreateTableAs", signature("PrestoConnection"), .sqlCreateTableAs)
