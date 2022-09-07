# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbWriteTable
#' @param overwrite a logical specifying whether to overwrite an existing table
#'   or not. Its default is `FALSE`.
#' @param append,field.types,temporary,row.names Ignored. Included for
#'   compatibility with
#'   generic.
#' @usage NULL
.dbWriteTable <- function(
  conn, name, value,
  overwrite = FALSE, ...,
  append = FALSE, field.types = NULL, temporary = FALSE, row.names = FALSE
) {
  stopifnot(
    length(overwrite) == 1 &&
      is.logical(overwrite) &&
      !is.na(overwrite)
  )
  stopifnot(is.data.frame(value))

  if (!identical(append, FALSE)) {
    stop('Appending not supported by RPresto yet', call. = FALSE)
  }
  if (!is.null(field.types)) {
    stop('`field.types` not supported by RPresto', call. = FALSE)
  }
  if (!identical(temporary, FALSE)) {
    stop('Temporary tables not supported by RPresto', call. = FALSE)
  }
  if (!identical(row.names, FALSE)) {
    stop('row.names not supported by RPresto', call. = FALSE)
  }

  sql <- dbplyr::sql_render(
    query = dbplyr::lazy_query(
      query_type = 'values', x = value,
      group_vars = character(), order_vars = NULL, frame = NULL
    ),
    con = conn
  )
  if (dbExistsTable(conn, name)) {
    if (identical(overwrite, TRUE)) {
      # Rename the existing table to a temporary random name
      # Create the table using the name
      # Remove the existing table and let the user know that the table is
      # overwritten
      rn <- paste0(
        'temp_', paste(sample(letters, 10, replace = TRUE), collapse = '')
      )
      DBI::dbExecute(
        conn,
        dbplyr::sql(paste('ALTER TABLE', name, 'RENAME TO', rn))
      )
      tryCatch(
        {
          dbCreateTableAs(conn, name, sql)
        },
        error = function(e) {
          # In case of error, revert the original table's name
          DBI::dbExecute(
            conn,
            dbplyr::sql(paste('ALTER TABLE', rn, 'RENAME TO', name))
          )
          stop(
            'Overwriting table ', name, ' failed with error: "',
            conditionMessage(e), '".', call. = FALSE
          )
        }
      )
      dbRemoveTable(conn, rn)
      message('The table ', name, ' is overwritten.')
    } else {
      stop(
        'The table ', name, ' exists but overwrite is set to FALSE.',
        call. = FALSE
      )
    }
  } else {
    dbCreateTableAs(conn, name, sql)
  }
  invisible(TRUE)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbWriteTable
#' @export
setMethod(
  'dbWriteTable',
  c('PrestoConnection', 'character', 'data.frame'),
  .dbWriteTable
)
