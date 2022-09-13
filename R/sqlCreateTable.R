# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::sqlCreateTable
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' @usage NULL
.sqlCreateTable <- function(con, table, fields, row.names = NA, temporary = FALSE, with = NULL, ...) {
  table <- DBI::dbQuoteIdentifier(con, table)

  if (is.data.frame(fields)) {
    fields <- DBI::sqlRownamesToColumn(fields, row.names)
    fields <- vapply(fields, function(x) DBI::dbDataType(con, x), character(1))
  }

  field_names <- DBI::dbQuoteIdentifier(con, names(fields))
  field_types <- unname(fields)
  fields <- paste0(field_names, " ", field_types)

  DBI::SQL(paste0(
    "CREATE ", if (temporary) "TEMPORARY ", "TABLE ", table, " (\n",
    "  ", paste(fields, collapse = ",\n  "), "\n)\n", with
  ))
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI sqlCreateTable
#' @export
setMethod("sqlCreateTable", signature("PrestoConnection"), .sqlCreateTable)
