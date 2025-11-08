# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

# Helper functions for column matching and reordering ------------------------

#' Get column names from a table
#'
#' @param conn PrestoConnection object
#' @param name Table name
#' @return Character vector of column names in table order
#' @keywords internal
get_table_columns <- function(conn, name) {
  DBI::dbListFields(conn, name)
}

#' Get column names from a SQL query or tbl_presto object
#'
#' @param conn PrestoConnection object
#' @param sql SQL string or tbl_presto object
#' @return Character vector of column names in query order
#' @keywords internal
get_query_columns <- function(conn, sql) {
  if (inherits(sql, "tbl_presto") || (is.list(sql) && !is.null(sql$lazy_query))) {
    # For tbl_presto: use op_vars
    dbplyr::op_vars(sql$lazy_query)
  } else {
    # For SQL string: use WHERE 1 = 0 to get empty result
    # This expands SELECT * and resolves aliases
    query_sql <- paste0("SELECT * FROM (", sql, ") WHERE 1 = 0")
    result <- DBI::dbGetQuery(conn, query_sql)
    colnames(result)
  }
}

#' Validate that column names match between table and query
#'
#' @param table_cols Character vector of table column names
#' @param query_cols Character vector of query column names
#' @return Invisibly returns TRUE if match, otherwise throws error
#' @keywords internal
validate_column_match <- function(table_cols, query_cols) {
  if (!setequal(table_cols, query_cols)) {
    missing <- setdiff(table_cols, query_cols)
    extra <- setdiff(query_cols, table_cols)
    
    error_msg <- "Column mismatch detected:"
    
    if (length(missing) > 0) {
      # Find indices of missing columns in table
      missing_indices <- which(table_cols %in% missing)
      missing_with_indices <- paste0(
        missing, " (index ", missing_indices, ")"
      )
      error_msg <- paste0(
        error_msg, "\n",
        "  Missing in query: ", paste(missing_with_indices, collapse = ", ")
      )
    }
    
    if (length(extra) > 0) {
      # Find indices of extra columns in query
      extra_indices <- which(query_cols %in% extra)
      extra_with_indices <- paste0(
        extra, " (index ", extra_indices, ")"
      )
      error_msg <- paste0(
        error_msg, "\n",
        "  Extra in query: ", paste(extra_with_indices, collapse = ", ")
      )
    }
    
    stop(error_msg, call. = FALSE)
  }
  invisible(TRUE)
}

#' Reorder SELECT columns to match target order
#'
#' @param conn PrestoConnection object
#' @param sql SQL string or tbl_presto object
#' @param target_order Character vector of column names in target order
#' @return SQL string or tbl_presto object with reordered columns
#' @keywords internal
reorder_select_columns <- function(conn, sql, target_order) {
  # Convert to tbl_presto if SQL string
  is_sql_string <- is.character(sql)
  
  if (is_sql_string) {
    # Convert SQL string to tbl_presto
    sql_tbl <- dplyr::tbl(conn, dbplyr::sql(sql))
  } else {
    # Already tbl_presto
    sql_tbl <- sql
  }
  
  # Reorder columns using select
  reordered_tbl <- dplyr::select(sql_tbl, !!!rlang::syms(target_order))
  
  # Convert back to SQL if original was SQL string
  if (is_sql_string) {
    dbplyr::sql_render(reordered_tbl, con = conn)
  } else {
    # Return tbl_presto
    reordered_tbl
  }
}

#' Compose query to append to a table using a statement
#'
#' @param con A database connection.
#' @param name The table name, passed on to [DBI::dbQuoteIdentifier()]. Options are:
#'   - a character string with the unquoted DBMS table name,
#'     e.g. `"table_name"`,
#'   - a call to [DBI::Id()] with components to the fully qualified table name,
#'     e.g. `Id(schema = "my_schema", table = "table_name")`
#'   - a call to [DBI::SQL()] with the quoted and fully qualified table name
#'     given verbatim, e.g. `SQL('"my_schema"."table_name"')`
#' @param sql a character string containing SQL statement, or a `tbl_presto` object.
#' @param auto_reorder Logical (default `TRUE`). If `TRUE`, automatically reorders
#'   query columns to match table column order when column names match but order
#'   differs. If `FALSE`, columns are inserted in query order.
#' @param ... Other arguments used by individual methods.
#' @export
#' @md
setGeneric("sqlAppendTableAs",
  def = function(con, name, sql, auto_reorder = TRUE, ...) {
    standardGeneric("sqlAppendTableAs")
  }
)

#' @rdname PrestoConnection-class
#' @usage NULL
.sqlAppendTableAs <- function(con, name, sql, auto_reorder = TRUE, ...) {
  # Validate sql is character or tbl_presto
  if (!(is.character(sql) || inherits(sql, "tbl_presto") || 
        (is.list(sql) && !is.null(sql$lazy_query)))) {
    stop(
      "sql must be a character string or a tbl_presto object.",
      call. = FALSE
    )
  }
  
  # If character, validate it's a single string
  if (is.character(sql)) {
    stopifnot(length(sql) == 1)
  }
  
  # Validate auto_reorder
  stopifnot(
    length(auto_reorder) == 1,
    is.logical(auto_reorder),
    !is.na(auto_reorder)
  )
  
  # INSERT INTO requires the table to exist
  if (!DBI::dbExistsTable(con, name)) {
    stop(
      "Table ", name, " does not exist. ",
      "INSERT INTO requires an existing table. ",
      "Use CREATE TABLE or dbCreateTableAs() to create the table first.",
      call. = FALSE
    )
  }
  
  # Get column names
  table_cols <- get_table_columns(con, name)
  query_cols <- get_query_columns(con, sql)
  
  # Validate column names match (order-independent)
  validate_column_match(table_cols, query_cols)
  
  # Handle column reordering if needed
  if (auto_reorder && !identical(table_cols, query_cols)) {
    # Same names, different order - reorder
    # Find columns that are out of order
    out_of_order <- which(table_cols != query_cols)
    
    if (length(out_of_order) > 0) {
      # Show columns that are out of order with original and new indices
      out_of_order_cols <- table_cols[out_of_order]
      original_indices <- match(out_of_order_cols, query_cols)
      new_indices <- out_of_order
      
      out_of_order_info <- paste0(
        out_of_order_cols, " (index ", original_indices, " -> ", new_indices, ")"
      )
      
      if (length(out_of_order) <= 10) {
        # Show all out-of-order columns if 10 or fewer
        reorder_msg <- paste(
          "Column order mismatch detected. Reordering",
          length(out_of_order), "column(s) to match table order:\n",
          "  ", paste(out_of_order_info, collapse = ", ")
        )
      } else {
        # Show first 5 and last 5 if more than 10
        first_five <- paste(out_of_order_info[1:5], collapse = ", ")
        last_five <- paste(
          out_of_order_info[(length(out_of_order) - 4):length(out_of_order)],
          collapse = ", "
        )
        reorder_msg <- paste(
          "Column order mismatch detected. Reordering",
          length(out_of_order), "column(s) to match table order:\n",
          "  ", first_five, ", ... (", length(out_of_order) - 10,
          " more), ... ", last_five
        )
      }
    } else {
      # Fallback (shouldn't happen if !identical, but just in case)
      reorder_msg <- paste(
        "Column order mismatch detected. Reordering columns to match table order."
      )
    }
    
    message(reorder_msg)
    sql <- reorder_select_columns(con, sql, table_cols)
  }
  
  # Handle tbl_presto objects - convert to SQL after reordering
  if (inherits(sql, "tbl_presto") || (is.list(sql) && !is.null(sql$lazy_query))) {
    sql <- dbplyr::sql_render(sql, con = con, ...)
  }
  
  # Validate sql is a character string
  stopifnot(is.character(sql), length(sql) == 1)
  
  # Handle table name
  if (inherits(name, "dbplyr_table_path")) { # dbplyr >= 2.5.0
    name <- dbplyr::table_path_name(name, con)
  } else {
    name <- DBI::dbQuoteIdentifier(con, name)
  }

  DBI::SQL(paste0(
    "INSERT INTO ", name, "\n",
    sql
  ))
}

#' @rdname PrestoConnection-class
#' @export
setMethod("sqlAppendTableAs", signature("PrestoConnection"), .sqlAppendTableAs)

