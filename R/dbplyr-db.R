# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Inform the dbplyr version used in this package
#'
#' @importFrom dbplyr dbplyr_edition
#' @export
#' @param con A DBIConnection object.
dbplyr_edition.PrestoConnection <- function(con) 2L

.description_from_info <- function(info) {
  return(paste0(
    "presto ",
    " [",
    info[["schema"]],
    ":",
    info[["catalog"]],
    " | ",
    info[["user"]],
    "@",
    info[["host"]],
    ":",
    info[["port"]],
    "]"
  ))
}

#' S3 implementation of \code{db_desc} for Presto.
#'
#' @importFrom dplyr db_desc
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_desc.PrestoConnection <- function(x) {
  info <- dbGetInfo(x)
  return(.description_from_info(info))
}

#' S3 implementation of \code{\link[dplyr]{db_data_type}} for Presto.
#'
#' @importFrom dplyr db_data_type
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_data_type.PrestoConnection <- function(con, fields, ...) {
  return(sapply(fields, function(field) dbDataType(Presto(), field)))
}

#' S3 implementation of \code{\link[dplyr]{db_explain}} for Presto.
#'
#' @importFrom dplyr db_explain
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_explain.PrestoConnection <- function(con, sql, ...) {
  explain.sql <- dbplyr::build_sql("EXPLAIN ", sql, con = con)
  explanation <- DBI::dbGetQuery(con, explain.sql)
  return(paste(explanation[[1]], collapse = "\n"))
}

#' S3 implementation of \code{\link[dplyr]{db_query_rows}} for Presto.
#'
#' @importFrom dplyr db_query_rows
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_query_rows.PrestoConnection <- function(con, sql) {
  # We shouldn't be doing a COUNT(*) over arbitrary tables because Hive tables
  # can be prohibitively long. There may be something smarter we can do for
  # smaller tables though.
  return(NA)
}

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
db_write_table.PrestoConnection <- function(con, table, types, values, temporary = FALSE, overwrite = FALSE,
                                            ..., with = NULL) {
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

#' @rdname dbplyr-db
#' @param unique_indexes,indexes,analyze,in_transaction Ignored. Included
#'   for compatibility with generics.
#' @param ... Extra arguments to be passed to individual methods.
#' @importFrom dbplyr db_copy_to
#' @export
db_copy_to.PrestoConnection <- function(con, table, values, overwrite = FALSE, types = NULL, temporary = TRUE,
                                        unique_indexes = NULL, indexes = NULL, analyze = TRUE, ...,
                                        in_transaction = TRUE, with = NULL) {
  table <- dplyr::db_write_table(
    con, table,
    types = types, values = values,
    temporary = temporary, overwrite = overwrite, with = with,
    ...
  )
  table
}

#' @rdname dbplyr-db
#' @param sql A SQL statement.
#' @importFrom dbplyr db_compute
#' @export
db_compute.PrestoConnection <- function(con, table, sql, temporary = TRUE, unique_indexes = list(), indexes = list(),
                                        analyze = TRUE, with = NULL,
                                        ...) {
  if (!identical(temporary, FALSE)) {
    stop(
      "Temporary table is not supported. ",
      "Use temporary = FALSE to create a permanent table.",
      call. = FALSE
    )
  }
  table <- dplyr::db_save_query(
    con, sql, table,
    temporary = temporary, with = with, ...
  )
  table
}

#' S3 implementation of \code{db_collect} for Presto.
#'
#' @importFrom dbplyr db_collect
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_collect.PrestoConnection <- function(con, sql, n = -1, warn_incomplete = TRUE, ...) {
  # This is the one difference between this implementation and the default
  # dbplyr::db_collect.DBIConnection()
  # We pass ... to dbSendQuery() so that bigint can be specified for individual
  # db_collect() calls
  res <- dbSendQuery(con, sql, ...)
  tryCatch(
    {
      out <- dbFetch(res, n = n)
    },
    finally = {
      dbClearResult(res)
    }
  )
  out
}
