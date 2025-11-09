# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

# Public API ---------------------------------------------------------------

#' Get column type information for a Presto table or query
#'
#' Returns column type information for a `tbl_presto` object, including
#' Presto types for complex and nested types.
#'
#' @param data A `tbl_presto` object
#' @param ... Additional arguments (currently unused)
#' @return A data frame with columns:
#'   - name: Column name (character)
#'   - type: R type (character)
#'   - .presto_type: Presto type as string (character)
#'
#' @importFrom dbplyr remote_con db_sql_render
#' @importFrom DBI dbSendQuery dbClearResult dbFetch dbColumnInfo
#' @export
#' @examples
#' \dontrun{
#' # Connect to Presto
#' con <- DBI::dbConnect(RPresto::Presto(), ...)
#' 
#' # Get column types for a table
#' tbl(con, "my_table") %>%
#'   presto_type()
#' 
#' # Get column types for a query
#' tbl(con, "my_table") %>%
#'   dplyr::filter(id > 100) %>%
#'   presto_type()
#' }
presto_type <- function(data, ...) {
  UseMethod("presto_type")
}

#' @rdname presto_type
#' @export
presto_type.tbl_presto <- function(data, ...) {
  # Get the connection from the tbl_presto
  conn <- dbplyr::remote_con(data)
  
  # Render the SQL from the tbl_presto
  sql <- dbplyr::db_sql_render(conn, data)
  
  # Execute the query with WHERE 1 = 0 to get column information
  # This is similar to how dbListFields handles PrestoResult
  query <- paste("SELECT * FROM (", sql, ") WHERE 1 = 0")
  result <- dbSendQuery(conn, query)
  on.exit(dbClearResult(result), add = TRUE)
  dbFetch(result, n = -1)
  
  # Use dbColumnInfo to get column type information
  # This uses the same helper function as dbColumnType
  return(DBI::dbColumnInfo(result))
}

