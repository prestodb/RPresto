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
#' @param name The table name, passed on to [DBI::dbQuoteIdentifier()]. Options are:
#'   - a character string with the unquoted DBMS table name,
#'     e.g. `"table_name"`,
#'   - a call to [DBI::Id()] with components to the fully qualified table name,
#'     e.g. `Id(schema = "my_schema", table = "table_name")`
#'   - a call to [DBI::SQL()] with the quoted and fully qualified table name
#'     given verbatim, e.g. `SQL('"my_schema"."table_name"')`
#' @param sql a character string containing SQL statement.
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' @param ... Other arguments used by individual methods.
#' @export
#' @md
setGeneric("sqlCreateTableAs",
  def = function(con, name, sql, with = NULL, ...) {
    standardGeneric("sqlCreateTableAs")
  }
)

#' @rdname PrestoConnection-class
#' @usage NULL
.sqlCreateTableAs <- function(con, name, sql, with = NULL, ...) {
  if (inherits(name, "dbplyr_table_path")) { # dbplyr >= 2.5.0
    name <- dbplyr::table_path_name(name, con)
  } else {
    name <- DBI::dbQuoteIdentifier(con, name)
  }

  DBI::SQL(paste0(
    "CREATE TABLE ", name, "\n",
    if (!is.null(with)) paste0(with, "\n"),
    "AS\n",
    sql
  ))
}

#' @rdname PrestoConnection-class
#' @export
setMethod("sqlCreateTableAs", signature("PrestoConnection"), .sqlCreateTableAs)
