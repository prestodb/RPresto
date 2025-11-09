# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R PrestoResult.R
NULL

# Internal helper function to check if a typeSignature is ARRAY of
# ARRAY
#
# @param type_signature A typeSignature object from Presto response
# @return TRUE if the type is ARRAY of ARRAY, FALSE otherwise
# @keywords internal
is_array_of_array <- function(type_signature) {
  if (is.null(type_signature) || is.null(type_signature$rawType)) {
    return(FALSE)
  }
  
  if (type_signature$rawType != "array") {
    return(FALSE)
  }
  
  # Get the element type
  element_type_sig <- type_signature$typeArguments[[1]] %||%
    type_signature$arguments[[1]]$value
  
  if (is.null(element_type_sig)) {
    return(FALSE)
  }
  
  # Check if element type is also an array
  element_raw_type <- element_type_sig$rawType
  return(!is.null(element_raw_type) && element_raw_type == "array")
}

# Internal helper function to format Presto type signature as string
#
# @param type_signature A typeSignature object from Presto response
# @return A character string representing the Presto type (e.g.,
#   "ARRAY<INTEGER>", "MAP<VARCHAR,INTEGER>",
#   "ROW(f1 INTEGER, f2 VARCHAR)")
# @keywords internal
format_presto_type_string <- function(type_signature) {
  if (is.null(type_signature) || is.null(type_signature$rawType)) {
    return("UNKNOWN")
  }
  
  raw_type <- type_signature$rawType
  raw_type_upper <- toupper(raw_type)
  
  # For simple types, just return the uppercase rawType
  if (!raw_type %in% c("array", "map", "row")) {
    return(raw_type_upper)
  }
  
  # For array types: ARRAY<element_type>
  if (raw_type == "array") {
    element_type_sig <- type_signature$typeArguments[[1]] %||%
      type_signature$arguments[[1]]$value
    if (is.null(element_type_sig)) {
      return("ARRAY<UNKNOWN>")
    }
    element_type <- format_presto_type_string(element_type_sig)
    return(paste0("ARRAY<", element_type, ">"))
  }
  
  # For map types: MAP<key_type,value_type>
  if (raw_type == "map") {
    key_type_sig <- type_signature$typeArguments[[1]] %||%
      type_signature$arguments[[1]]$value
    value_type_sig <- type_signature$typeArguments[[2]] %||%
      type_signature$arguments[[2]]$value
    
    if (is.null(key_type_sig) || is.null(value_type_sig)) {
      return("MAP<UNKNOWN,UNKNOWN>")
    }
    
    key_type <- format_presto_type_string(key_type_sig)
    value_type <- format_presto_type_string(value_type_sig)
    return(paste0("MAP<", key_type, ",", value_type, ">"))
  }
  
  # For row types: ROW(field1 type1, field2 type2, ...)
  if (raw_type == "row") {
    fields <- type_signature$arguments
    if (is.null(fields) || length(fields) == 0) {
      return("ROW()")
    }
    
    field_strings <- purrr::map_chr(seq_along(fields), function(i) {
      field <- fields[[i]]
      
      # Get field name
      field_name <- purrr::pluck(field, "value", "fieldName", "name") %||%
        purrr::pluck(type_signature, "literalArguments", i) %||%
        paste0("field", i)
      
      # Get field type
      field_type_sig <- purrr::pluck(field, "value", "typeSignature")
      if (is.null(field_type_sig)) {
        field_type <- "UNKNOWN"
      } else if (is.character(field_type_sig)) {
        # Field type is a direct string (e.g., "integer", "array(integer)",
        # or "map(varchar,integer)")
        # Convert parentheses to angle brackets for ARRAY and MAP types
        field_type_upper <- toupper(field_type_sig)
        # Replace ARRAY( with ARRAY< and MAP( with MAP<, and closing ) with >
        # Handle both simple ARRAY(INTEGER) and nested ARRAY(ARRAY(INTEGER))
        if (startsWith(field_type_upper, "ARRAY(")) {
          # Replace all ARRAY( with ARRAY< and all ) with >
          field_type <- gsub("ARRAY\\(", "ARRAY<", field_type_upper)
          field_type <- gsub("\\)", ">", field_type)
        } else if (startsWith(field_type_upper, "MAP(")) {
          # Replace MAP( with MAP< and all ) with >
          # Handle MAP(VARCHAR,INTEGER) -> MAP<VARCHAR,INTEGER>
          field_type <- gsub("MAP\\(", "MAP<", field_type_upper)
          field_type <- gsub("\\)", ">", field_type)
        } else {
          field_type <- field_type_upper
        }
      } else {
        # Field type is a typeSignature object
        field_type <- format_presto_type_string(field_type_sig)
      }
      
      return(paste0(field_name, " ", field_type))
    })
    
    return(paste0("ROW(", paste(field_strings, collapse = ", "), ")"))
  }
  
  return(raw_type_upper)
}

# Internal helper function to map Presto type string to R type string
#
# @param presto_type A Presto type string (e.g., "INTEGER", "VARCHAR",
#   "ARRAY<INTEGER>")
# @return An R type string (e.g., "integer", "character", "list")
# @keywords internal
presto_type_to_r_type <- function(presto_type) {
  if (is.null(presto_type) || is.na(presto_type) || presto_type == "UNKNOWN") {
    return("character")
  }
  
  presto_type_upper <- toupper(presto_type)
  
  # Complex types (ARRAY, MAP, ROW) map to list
  if (startsWith(presto_type_upper, "ARRAY") ||
      startsWith(presto_type_upper, "MAP") ||
      startsWith(presto_type_upper, "ROW")) {
    return("list")
  }
  
  # Map primitive types
  switch(
    presto_type_upper,
    BOOLEAN = "logical",
    TINYINT = "integer",
    SMALLINT = "integer",
    INTEGER = "integer",
    BIGINT = "integer",
    REAL = "numeric",
    DOUBLE = "numeric",
    DECIMAL = "character",
    VARCHAR = "character",
    CHAR = "character",
    VARBINARY = "raw",
    JSON = "character",
    DATE = "Date",
    TIME = "difftime",
    "TIME WITH TIME ZONE" = "difftime",
    TIMESTAMP = "POSIXct",
    "TIMESTAMP WITH TIME ZONE" = "POSIXct",
    "INTERVAL YEAR TO MONTH" = "Duration",
    "INTERVAL DAY TO SECOND" = "Duration",
    # Default to character for unknown types
    "character"
  )
}


# Internal helper function to extract column information from a PrestoResult
#
# @param res A PrestoResult object
# @return A data frame with columns:
#   - name: Column name (character)
#   - type: R type (character)
#   - .presto_type: Presto type as string (character)
# @keywords internal
.get_column_info_from_result <- function(res) {
  if (!inherits(res, "PrestoResult")) {
    stop("res must be a PrestoResult object", call. = FALSE)
  }
  
  if (!dbIsValid(res)) {
    stop("The result object is not valid", call. = FALSE)
  }
  
  # Try to get column information from content if available
  content <- res@query$content()
  columns <- content$columns
  
  # If columns are available in content, extract them
  if (!is.null(columns) && length(columns) > 0) {
    col_names <- purrr::map_chr(columns, ~ .x$name)
    presto_types <- purrr::map_chr(columns, ~ {
      # Extract and format type from typeSignature
      if (!is.null(.x$typeSignature)) {
        format_presto_type_string(.x$typeSignature)
      } else {
        "UNKNOWN"
      }
    })
    r_types <- purrr::map_chr(presto_types, presto_type_to_r_type)
    
    return(data.frame(
      name = col_names,
      type = r_types,
      .presto_type = presto_types,
      stringsAsFactors = FALSE
    ))
  }
  
  # Fallback: Execute a new query with WHERE 1 = 0 to get column info
  # This is similar to what dbListFields does for PrestoResult
  new_query <- sprintf(
    "SELECT * FROM (%s) WHERE 1 = 0",
    res@statement
  )
  fallback_result <- dbSendQuery(res@connection, new_query)
  on.exit(dbClearResult(fallback_result), add = TRUE)
  
  # Fetch to ensure query completes and columns are available
  dbFetch(fallback_result, n = -1)
  
  # Get columns from the fallback result
  fallback_content <- fallback_result@query$content()
  fallback_columns <- fallback_content$columns
  
  if (is.null(fallback_columns) || length(fallback_columns) == 0) {
    stop(
      "Could not retrieve column information from result",
      call. = FALSE
    )
  }
  
  col_names <- purrr::map_chr(fallback_columns, ~ .x$name)
  presto_types <- purrr::map_chr(fallback_columns, ~ {
    if (!is.null(.x$typeSignature)) {
      format_presto_type_string(.x$typeSignature)
    } else {
      "UNKNOWN"
    }
  })
  r_types <- purrr::map_chr(presto_types, presto_type_to_r_type)
  
  return(data.frame(
    name = col_names,
    type = r_types,
    .presto_type = presto_types,
    stringsAsFactors = FALSE
  ))
}

#' Get column type information for a Presto table
#'
#' @param conn A PrestoConnection object
#' @param name A table name (character, Id, SQL, or dbplyr_schema)
#' @param ... Additional arguments (not currently used)
#' @return A data frame with columns:
#'   - name: Column name (character)
#'   - type: R type (character)
#'   - .presto_type: Presto type as string (character)
#' @rdname PrestoConnection-class
#' @export
dbColumnType <- function(conn, name, ...) {
  UseMethod("dbColumnType")
}

#' @rdname PrestoConnection-class
#' @usage NULL
.dbColumnType_PrestoConnection <- function(conn, name, ...) {
  # Quote the table name
  name <- DBI::dbQuoteIdentifier(conn, name)
  
  # Use SELECT * FROM ... WHERE 1 = 0 to get column type information
  # This approach uses PrestoResult's typeSignature which provides
  # complete type information for complex and nested types
  query <- paste("SELECT * FROM", name, "WHERE 1 = 0")
  result <- dbSendQuery(conn, query)
  on.exit(dbClearResult(result), add = TRUE)
  dbFetch(result, n = -1)
  
  # Use the shared helper function to get column info from PrestoResult
  return(.get_column_info_from_result(result))
}

#' @rdname PrestoConnection-class
#' @export
setMethod("dbColumnType", signature("PrestoConnection", "character"), .dbColumnType_PrestoConnection)

setOldClass("dbplyr_schema")

#' @rdname PrestoConnection-class
#' @export
setMethod("dbColumnType", signature("PrestoConnection", "dbplyr_schema"), .dbColumnType_PrestoConnection)

#' @rdname PrestoConnection-class
#' @export
setMethod("dbColumnType", signature("PrestoConnection", "Id"), .dbColumnType_PrestoConnection)

#' @rdname PrestoConnection-class
#' @export
setMethod("dbColumnType", signature("PrestoConnection", "SQL"), .dbColumnType_PrestoConnection)

