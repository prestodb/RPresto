# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbWriteTable
#' @param chunk.fields A character vector of names of the fields that should
#'   be used to slice the value data frame into chunks for batch append. This is
#'   necessary when the data frame is too big to be uploaded at once in one
#'   single INSERT INTO statement. Default to NULL which inserts the entire
#'   value data frame.
#' @param use.one.query A boolean to indicate if to use a single CREATE TABLE AS
#'   statement rather than the default implementation of using
#'   separate CREATE TABLE and INSERT INTO statements. Some Presto backends
#'   might have different requirements between the two approaches. e.g.
#'   INSERT INTO might not be allowed to mutate an unpartitioned table created
#'   by CREATE TABLE. If set to TRUE, chunk.fields cannot be used.
#' @usage NULL
.dbWriteTable <- function(conn, name, value,
                          overwrite = FALSE, ...,
                          append = FALSE, field.types = NULL, temporary = FALSE,
                          row.names = FALSE, with = NULL, chunk.fields = NULL,
                          use.one.query = FALSE) {
  force(with)
  force(chunk.fields)
  force(use.one.query)
  stopifnot(is.data.frame(value))
  if (!identical(temporary, FALSE)) {
    stop("Temporary tables not supported by RPresto", call. = FALSE)
  }

  if (is.null(row.names)) row.names <- FALSE
  if (
    (!is.logical(row.names) && !is.character(row.names)) ||
      (length(row.names) != 1L)
  ) {
    stop("row.names must be a logical scalar or a string", call. = FALSE)
  }
  if (!is.logical(overwrite) || length(overwrite) != 1L || is.na(overwrite)) {
    stop("overwrite must be a logical scalar", call. = FALSE)
  }

  if (use.one.query) {
    if (append) {
      stop("append and use.one.query cannot both be TRUE", call. = FALSE)
    }
    if (!is.null(field.types)) {
      stop(
        "field.types is not supported when use.one.query is TRUE",
        call. = FALSE
      )
    }
    if (!is.null(chunk.fields)) {
      stop(
        "chunk.fields is not supported when use.one.query is TRUE",
        call. = FALSE
      )
    }
  } else {
    if (
      (!is.null(field.types)) &&
        (!(is.character(field.types))) &&
        (!is.null(names(field.types))) &&
        (!anyDuplicated(names(field.types)))
    ) {
      stop(
        "field.types must be a named character vector with unique names, or NULL",
        call. = FALSE
      )
    }
    if (!is.logical(append) || length(append) != 1L || is.na(append)) {
      stop("append must be a logical scalar", call. = FALSE)
    }
    if (overwrite && append) {
      stop("overwrite and append cannot both be TRUE", call. = FALSE)
    }
    if (append && !is.null(field.types)) {
      stop("Cannot specify field.types with `append = TRUE`", call. = FALSE)
    }
  }

  found <- DBI::dbExistsTable(conn, name)
  if (found && !overwrite && !append) {
    stop("Table ", name, " exists in database, and both overwrite and",
      " append are FALSE",
      call. = FALSE
    )
  }
  rn <- paste0(
    "temp_", paste(sample(letters, 10, replace = TRUE), collapse = "")
  )
  if (found && overwrite) {
    # Without implementation of TRANSACTION, we play it safe by renaming
    # the to-be-overwritten table rather than deleting it right away
    dbRenameTable(conn, name, rn)
  }

  value <- DBI::sqlRownamesToColumn(value, row.names)
  tryCatch(
    {
      if (!found || overwrite) {
        if (use.one.query) {
          fields <- DBI::dbQuoteIdentifier(conn, colnames(value))
          sql <- DBI::SQL(paste0(
            "SELECT * FROM (\n",
            .create_values_statement(conn, value),
            ") AS t (", paste(fields, collapse = ", "), ")\n"
          ))
          dbCreateTableAs(
            conn = conn,
            name = name,
            sql = sql,
            overwrite = overwrite,
            with = with
          )
        } else {
          if (is.null(field.types)) {
            combined_field_types <- lapply(value, dbDataType, dbObj = Presto())
          } else {
            combined_field_types <- rep("", length(value))
            names(combined_field_types) <- names(value)
            field_types_idx <- match(
              names(field.types), names(combined_field_types)
            )
            stopifnot(!any(is.na(field_types_idx)))
            combined_field_types[field_types_idx] <- field.types
            values_idx <- setdiff(seq_along(value), field_types_idx)
            combined_field_types[values_idx] <- lapply(
              value[values_idx], dbDataType,
              dbObj = Presto()
            )
          }
          DBI::dbCreateTable(
            conn = conn,
            name = name,
            fields = combined_field_types,
            with = with,
            temporary = temporary
          )
        }
      }
      if (!use.one.query && nrow(value) > 0) {
        DBI::dbAppendTable(conn, name, value, chunk.fields = chunk.fields)
      }
    },
    error = function(e) {
      # In case of error, try revert the origin table
      if (dbExistsTable(conn, name) && !found) {
        dbRemoveTable(conn, name)
      }
      if (dbExistsTable(conn, rn)) {
        dbRenameTable(conn, rn, name)
      }
      stop(
        "Writing table ", name, ' failed with error: "',
        conditionMessage(e), '".',
        call. = FALSE
      )
    }
  )
  if (dbExistsTable(conn, rn)) {
    if (dbRemoveTable(conn, rn)) {
      message("The table ", name, " is overwritten.")
    }
  }

  invisible(TRUE)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbWriteTable
#' @export
setMethod(
  "dbWriteTable",
  signature("PrestoConnection", "ANY", "data.frame"),
  .dbWriteTable
)

.create_values_statement <- function(conn, value, row.names = FALSE) {
  sql_values <- DBI::sqlData(conn, value, row.names)
  rows <- do.call(paste, c(unname(sql_values), sep = ", "))
  DBI::SQL(paste0("VALUES\n", paste0("  (", rows, ")", collapse = ",\n")))
}
