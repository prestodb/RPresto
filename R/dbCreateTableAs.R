# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' Create a table in database using a statement
#'
#' @inheritParams DBI::dbReadTable
#' @inheritParams sqlCreateTableAs
#' @param sql a character string containing SQL statement.
#' @param overwrite A boolean indicating if an existing table should be
#'   overwritten. Default to FALSE.
#' @export
setGeneric("dbCreateTableAs",
  def = function(conn, name, sql, overwrite = FALSE, with = NULL, ...) {
    standardGeneric("dbCreateTableAs")
  }
)

#' @rdname PrestoConnection-class
#' @usage NULL
.dbCreateTableAs <- function(conn, name, sql, overwrite = FALSE, with = NULL, ...) {
  stopifnot(is.character(sql), length(sql) == 1)
  stopifnot(length(overwrite) == 1, is.logical(overwrite), !is.na(overwrite))

  query <- sqlCreateTableAs(
    con = conn,
    name = name,
    sql = sql,
    with = with,
    ...
  )
  if (dbExistsTable(conn, name)) {
    if (identical(overwrite, TRUE)) {
      # Rename the existing table to a temporary random name
      # Create the table using the name
      # Remove the existing table and let the user know that the table is
      # overwritten
      rn <- paste0(
        "temp_", paste(sample(letters, 10, replace = TRUE), collapse = "")
      )
      dbRenameTable(conn, name, rn)
      tryCatch(
        {
          DBI::dbExecute(conn, query)
        },
        error = function(e) {
          # In case of error, revert the original table's name
          dbRenameTable(conn, rn, name)
          stop(
            "Overwriting table ", name, ' failed with error: "',
            conditionMessage(e), '".',
            call. = FALSE
          )
        }
      )
      if (dbRemoveTable(conn, rn)) {
        message("The table ", name, " is overwritten.")
      }
    } else {
      stop(
        "The table ", name, " exists but overwrite is set to FALSE.",
        call. = FALSE
      )
    }
  } else {
    DBI::dbExecute(conn, query)
  }
  invisible(TRUE)
}

#' @rdname PrestoConnection-class
#' @export
setMethod(
  "dbCreateTableAs", signature("PrestoConnection"),
  .dbCreateTableAs
)
