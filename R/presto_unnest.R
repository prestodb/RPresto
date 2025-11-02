# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Create a lazy_unnest_query object
#'
#' @param x Parent lazy_query object
#' @param col Column name to unnest (character)
#' @param values_to Name of output column for unnested values
#' @param with_ordinality Whether to include ordinality column
#' @param ordinality_to Name of ordinality column
#' @keywords internal
new_lazy_unnest_query <- function(x, col, values_to = NULL, 
                                  with_ordinality = FALSE, 
                                  ordinality_to = NULL) {
  structure(
    list(
      x = x,
      col = col,
      values_to = values_to %||% col,  # Default to column name
      with_ordinality = with_ordinality,
      ordinality_to = ordinality_to
    ),
    class = c("lazy_unnest_query", "lazy_query")
  )
}

#' Build SQL for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param con Database connection
#' @param ... Additional arguments
#' @importFrom dbplyr sql_build sql_render
#' @keywords internal
#' @export
sql_build.lazy_unnest_query <- function(op, con, ...) {
  # Build the parent query
  # Call sql_build on the parent lazy_query
  # We use dbplyr::sql_build which dispatches correctly
  x <- dbplyr::sql_build(op$x, con, ...)
  
  # Check if we got a valid result
  if (is.null(x)) {
    stop("Parent query sql_build returned NULL", call. = FALSE)
  }
  
  # Return query object structure
  # This represents the query before SQL rendering
  # Include both classes so sql_render can dispatch correctly
  # whether called directly or via lazy_query
  structure(
    list(
      from = x,
      col = op$col,
      values_to = op$values_to,
      with_ordinality = op$with_ordinality,
      ordinality_to = op$ordinality_to
    ),
    class = c("unnest_query_build", "lazy_query")
  )
}

#' Check if a select_query is a simple table reference (no filters, groupings, etc.)
#'
#' @param query A select_query object
#' @return Logical indicating if the query is a simple table reference
#' @keywords internal
is_simple_table_reference <- function(query) {
  inherits(query, "select_query") &&
    !is.null(query$from) &&
    !inherits(query$from, c("select_query", "join_query", "set_op_query", "union_query")) &&
    is.null(query$where) && is.null(query$group_by) &&
    is.null(query$having) && is.null(query$order_by) &&
    is.null(query$limit) &&
    (is.null(query$select) || identical(query$select, dbplyr::sql("*")))
}

#' Get column names for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param ... Additional arguments
#' @importFrom dbplyr op_vars
#' @keywords internal
#' @export
op_vars.lazy_unnest_query <- function(op, ...) {
  # Get columns from parent query
  parent_vars <- dbplyr::op_vars(op$x)
  
  # Add the unnested column name
  vars <- c(parent_vars, op$values_to)
  
  # If with_ordinality, add the ordinality column
  if (op$with_ordinality && !is.null(op$ordinality_to)) {
    vars <- c(vars, op$ordinality_to)
  }
  
  return(vars)
}

#' Render SQL for lazy_unnest_query
#'
#' @param query Query object (either lazy_unnest_query or result of sql_build)
#' @param con Database connection (PrestoConnection)
#' @param ... Additional arguments
#' @importFrom dbplyr sql_build sql_render
#' @keywords internal
#' @export
sql_render.lazy_unnest_query <- function(query, con, ...) {
  # If query is still a lazy_unnest_query, build it first
  # This can happen when dbplyr calls sql_render directly on a lazy_query
  if (inherits(query, "lazy_unnest_query")) {
    # Don't pass ... to sql_build as it may contain render-specific parameters
    query <- dbplyr::sql_build(query, con)
  }
  
  # At this point, query should be an unnest_query_build object
  if (is.null(query$from)) {
    stop("Parent query is NULL in sql_render.lazy_unnest_query. Query structure: ", 
         paste(names(query), collapse = ", "), call. = FALSE)
  }
  
  # Check if query$from is a simple table reference (SELECT * FROM table without modifications)
  # If so, use the table reference directly to avoid redundant subquery wrapper
  if (is_simple_table_reference(query$from)) {
    # Use table reference directly without SELECT wrapper
    from_sql <- dbplyr::sql_render(query$from$from, con, ...)
    use_subquery <- FALSE
  } else {
    # For complex queries, wrap in subquery
    from_sql <- dbplyr::sql_render(query$from, con, ...)
    use_subquery <- TRUE
  }
  
  # Quote the column name - use ident directly, dbplyr will handle quoting
  col_quoted <- dbplyr::ident(query$col)
  
  # Build the UNNEST clause
  if (query$with_ordinality) {
    if (is.null(query$ordinality_to)) {
      stop("ordinality_to must be provided when with_ordinality = TRUE", 
           call. = FALSE)
    }
    # CROSS JOIN UNNEST(array_col) WITH ORDINALITY AS t(value, ordinality)
    # In Presto, WITH ORDINALITY syntax is: UNNEST(arr) WITH ORDINALITY AS t(value, ordinality)
    unnest_clause <- dbplyr::build_sql(
      "CROSS JOIN UNNEST(", col_quoted, ") WITH ORDINALITY AS t(",
      dbplyr::ident(query$values_to), 
      ", ", dbplyr::ident(query$ordinality_to), ")",
      con = con
    )
  } else {
    # CROSS JOIN UNNEST(array_col) AS t(value)
    # In Presto, UNNEST with alias syntax is: UNNEST(arr) AS t(column_name)
    # We need a table alias (t) and column name
    unnest_clause <- dbplyr::build_sql(
      "CROSS JOIN UNNEST(", col_quoted, ") AS t(",
      dbplyr::ident(query$values_to), ")",
      con = con
    )
  }
  
  # Build final SELECT * FROM (subquery) CROSS JOIN UNNEST...
  # Or SELECT * FROM table CROSS JOIN UNNEST... for simple queries
  if (use_subquery) {
    dbplyr::build_sql(
      "SELECT * FROM (", dbplyr::sql(from_sql), ") ", unnest_clause,
      con = con
    )
  } else {
    dbplyr::build_sql(
      "SELECT * FROM ", dbplyr::sql(from_sql), " ", unnest_clause,
      con = con
    )
  }
}

#' Unnest array columns in Presto tables
#'
#' Expands array columns into rows using Presto's `CROSS JOIN UNNEST` syntax.
#' This is similar to `tidyr::unnest()` but works with Presto database tables.
#'
#' @param data A `tbl_presto` object
#' @param cols Column(s) to unnest. Currently only supports a single column.
#' @param ... Additional arguments (currently unused)
#' @param values_to Name of column to store unnested values. If `NULL`, uses
#'   the original column name.
#' @param with_ordinality If `TRUE`, includes an ordinality column with the
#'   position of each element in the array.
#' @param ordinality_to Name of ordinality column when `with_ordinality = TRUE`.
#'   Must be provided if `with_ordinality = TRUE`.
#'
#' @return A `tbl_presto` object with the array column unnested into rows.
#'
#' @importFrom rlang enquo
#' @importFrom stats setNames
#' @importFrom tidyselect eval_select
#' @export
#' @examples
#' \dontrun{
#' # Connect to Presto
#' con <- DBI::dbConnect(RPresto::Presto(), ...)
#' 
#' # Create a table with an array column
#' DBI::dbExecute(con, "CREATE TABLE test (id BIGINT, arr ARRAY(BIGINT))")
#' DBI::dbExecute(con, "INSERT INTO test VALUES (1, ARRAY[10, 20, 30])")
#' 
#' # Unnest the array column
#' tbl(con, "test") %>%
#'   presto_unnest(arr, values_to = "elem") %>%
#'   collect()
#' }
presto_unnest <- function(data, cols, ..., 
                          values_to = NULL,
                          with_ordinality = FALSE,
                          ordinality_to = NULL) {
  UseMethod("presto_unnest")
}

#' @rdname presto_unnest
#' @export
presto_unnest.tbl_presto <- function(data, cols, ..., 
                                     values_to = NULL,
                                     with_ordinality = FALSE,
                                     ordinality_to = NULL) {
  # Get available column names from the data - this is our source of truth
  available_cols <- colnames(data)
  
  # Extract column selections using tidyselect
  # Since we control this method completely, cols will only contain actual column selections
  cols_quo <- rlang::enquo(cols)
  col_names_raw <- tryCatch({
    tidyselect::eval_select(cols_quo, data)
  }, error = function(e) {
    # If evaluation failed, try extracting from expression directly
    cols_expr <- rlang::quo_get_expr(cols_quo)
    if (rlang::is_symbol(cols_expr)) {
      col_name <- as.character(cols_expr)
      if (!col_name %in% available_cols) {
        stop(sprintf("Column '%s' not found in data", col_name), call. = FALSE)
      }
      return(setNames(which(available_cols == col_name), col_name))
    }
    stop("Could not determine column to unnest", call. = FALSE)
  })
  
  # Validate that selected columns actually exist in the data
  # This provides a clear error message if invalid columns are selected
  valid_col_names <- intersect(names(col_names_raw), available_cols)
  
  if (length(valid_col_names) == 0) {
    stop("No valid columns found to unnest. Did you pass column selection correctly?", 
         call. = FALSE)
  } else if (length(valid_col_names) > 1) {
    stop("Currently only one column can be unnested at a time", call. = FALSE)
  }
  
  col_name <- valid_col_names[1]
  
  # Determine output column name
  if (is.null(values_to)) {
    values_to <- col_name  # Default to same name as input column
  }
  
  # Validate ordinality parameters
  if (with_ordinality && is.null(ordinality_to)) {
    stop("ordinality_to must be provided when with_ordinality = TRUE", 
         call. = FALSE)
  }
  
  # Create new lazy query and return updated tbl_presto
  data$lazy_query <- new_lazy_unnest_query(
    x = data$lazy_query,
    col = col_name,
    values_to = values_to,
    with_ordinality = with_ordinality,
    ordinality_to = ordinality_to
  )
  
  return(data)
}
