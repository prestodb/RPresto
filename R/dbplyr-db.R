# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' dbplyr database methods
#' 
#' @rdname dbplyr-db
#' @param con A `PrestoConnection` as returned by `dbConnect()`.
#' @importFrom dplyr db_list_tables
#' @export
#' @md
db_list_tables.PrestoConnection <- function(con) {
  DBI::dbListTables(con)
}

#' @rdname dbplyr-db
#' @param table Table name
#' @importFrom dplyr db_has_table
#' @export
db_has_table.PrestoConnection <- function(con, table) {
  DBI::dbExistsTable(con, table)
}

#' @rdname dbplyr-db
#' @param types Not used. Only NULL is accpeted.
#' @param values A `data.frame`.
#' @param temporary If a temporary table should be used. Not supported. Only
#'   FALSE is accepted.
#' @param overwrite If an existing table should be overwritten.
#' @param with An optional WITH clause for the CREATE TABLE statement.
#' @importFrom dplyr db_write_table
#' @export
db_write_table.PrestoConnection  <- function(
  con, table, types, values, temporary = FALSE, overwrite = FALSE,
  ..., with = NULL
) {
  dbWriteTable(
    conn = con,
    name = table,
    value = values,
    field.types = types,
    temporary = temporary,
    overwrite = overwrite,
    with = with,
    ...
  )
  table
}
