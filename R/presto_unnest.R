# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

# Public API ---------------------------------------------------------------

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
  available_cols <- colnames(data)
  cols_quo <- rlang::enquo(cols)
  col_names_raw <- tryCatch({
    tidyselect::eval_select(cols_quo, data)
  }, error = function(e) {
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
  
  valid_col_names <- intersect(names(col_names_raw), available_cols)
  
  if (length(valid_col_names) == 0) {
    stop("No valid columns found to unnest. Did you pass column selection correctly?", 
         call. = FALSE)
  } else if (length(valid_col_names) > 1) {
    stop("Currently only one column can be unnested at a time", call. = FALSE)
  }
  
  col_name <- valid_col_names[1]
  
  if (is.null(values_to)) {
    values_to <- col_name
  }
  
  if (with_ordinality && is.null(ordinality_to)) {
    stop("ordinality_to must be provided when with_ordinality = TRUE", 
         call. = FALSE)
  }
  
  data$lazy_query <- structure(
    list(
      x = data$lazy_query,
      col = col_name,
      values_to = values_to,
      with_ordinality = with_ordinality,
      ordinality_to = ordinality_to
    ),
    class = c("lazy_unnest_query", "lazy_query")
  )
  
  return(data)
}

# lazy_unnest_query methods ------------------------------------------------

#' Get column names for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param ... Additional arguments
#' @importFrom dbplyr op_vars
#' @keywords internal
#' @export
op_vars.lazy_unnest_query <- function(op, ...) {
  parent_vars <- dbplyr::op_vars(op$x)
  vars <- c(parent_vars, op$values_to)
  
  if (op$with_ordinality && !is.null(op$ordinality_to)) {
    vars <- c(vars, op$ordinality_to)
  }
  
  return(vars)
}

#' Get grouping variables for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param ... Additional arguments
#' @importFrom dbplyr op_grps
#' @keywords internal
#' @export
op_grps.lazy_unnest_query <- function(op, ...) {
  dbplyr::op_grps(op$x, ...)
}

#' Get sorting variables for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param ... Additional arguments
#' @importFrom dbplyr op_sort
#' @keywords internal
#' @export
op_sort.lazy_unnest_query <- function(op, ...) {
  dbplyr::op_sort(op$x, ...)
}

#' Get window frame for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param ... Additional arguments
#' @importFrom dbplyr op_frame
#' @keywords internal
#' @export
op_frame.lazy_unnest_query <- function(op, ...) {
  dbplyr::op_frame(op$x, ...)
}

#' Build SQL for lazy_unnest_query
#'
#' @param op lazy_unnest_query object
#' @param con Database connection
#' @param ... Additional arguments
#' @importFrom dbplyr sql_build sql_render sql_optimise
#' @keywords internal
#' @export
sql_build.lazy_unnest_query <- function(op, con, ...) {
  x <- dbplyr::sql_build(op$x, con, ...)
  
  if (is.null(x)) {
    stop("Parent query sql_build returned NULL", call. = FALSE)
  }
  
  structure(
    list(
      from = x,
      col = op$col,
      values_to = op$values_to,
      with_ordinality = op$with_ordinality,
      ordinality_to = op$ordinality_to
    ),
    class = c("unnest_query", "query")
  )
}

# unnest_query methods -----------------------------------------------------

#' Render SQL for unnest_query objects
#'
#' @param query unnest_query object
#' @param con Database connection (PrestoConnection)
#' @param ... Additional arguments
#' @importFrom dbplyr sql_render
#' @keywords internal
#' @export
sql_render.unnest_query <- function(query, con, ...) {
  if (is.null(query$from)) {
    stop("Parent query is NULL in sql_render.unnest_query. Query structure: ", 
         paste(names(query), collapse = ", "), call. = FALSE)
  }
  
  from_sql <- dbplyr::sql_render(query$from, con, ...)
  col_quoted <- dbplyr::ident(query$col)
  
  if (query$with_ordinality) {
    if (is.null(query$ordinality_to)) {
      stop("ordinality_to must be provided when with_ordinality = TRUE", 
           call. = FALSE)
    }
    unnest_clause <- dbplyr::build_sql(
      "CROSS JOIN UNNEST(", col_quoted, ") WITH ORDINALITY AS t(",
      dbplyr::ident(query$values_to), 
      ", ", dbplyr::ident(query$ordinality_to), ")",
      con = con
    )
  } else {
    unnest_clause <- dbplyr::build_sql(
      "CROSS JOIN UNNEST(", col_quoted, ") AS t(",
      dbplyr::ident(query$values_to), ")",
      con = con
    )
  }
  
  dbplyr::build_sql(
    "SELECT * FROM (", dbplyr::sql(from_sql), ") ", unnest_clause,
    con = con
  )
}
