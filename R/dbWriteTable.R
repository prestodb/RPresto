# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbWriteTable
#' @param chunk_fields A character vector of names of the fields that should
#'   be used to slice the value data frame into chunks for batch append.
#'   Default to NULL which appends the entire value data frame.
#' @usage NULL
.dbWriteTable <- function(conn, name, value,
                          overwrite = FALSE, ...,
                          append = FALSE, field.types = NULL, temporary = FALSE,
                          row.names = FALSE, with = NULL, chunk_fields = NULL) {
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
  if (!is.logical(append) || length(append) != 1L || is.na(append)) {
    stop("append must be a logical scalar", call. = FALSE)
  }
  if (overwrite && append) {
    stop("overwrite and append cannot both be TRUE", call. = FALSE)
  }
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
  if (append && !is.null(field.types)) {
    stop("Cannot specify field.types with `append = TRUE`", call. = FALSE)
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
    DBI::dbExecute(
      conn,
      DBI::SQL(paste("ALTER TABLE", name, "RENAME TO", rn))
    )
  }

  tryCatch(
    {
      value <- DBI::sqlRownamesToColumn(value, row.names)

      if (!found || overwrite) {
        if (is.null(field.types)) {
          combined_field_types <- lapply(value, dbDataType, dbObj = Presto())
        } else {
          combined_field_types <- rep("", length(value))
          names(combined_field_types) <- names(value)
          field_types_idx <- match(names(field.types), names(combined_field_types))
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

      if (nrow(value) > 0) {
        DBI::dbAppendTable(conn, name, value, chunk_fields = chunk_fields)
      }
    },
    error = function(e) {
      # In case of error, try revert the origin table
      if (dbExistsTable(conn, name)) {
        dbRemoveTable(conn, name)
      }
      if (dbExistsTable(conn, rn)) {
        DBI::dbExecute(
          conn,
          DBI::SQL(paste("ALTER TABLE", rn, "RENAME TO", name))
        )
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
  c("PrestoConnection", "character", "data.frame"),
  .dbWriteTable
)
