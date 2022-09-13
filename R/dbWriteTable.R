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
.dbWriteTable <- function(conn, name, value,
                          overwrite = FALSE, ...,
                          append = FALSE, field.types = NULL, temporary = FALSE, row.names = FALSE,
                          with = NULL) {
  stopifnot(is.data.frame(value))

  if (!identical(append, FALSE)) {
    stop("Appending not supported by RPresto yet", call. = FALSE)
  }
  if (!is.null(field.types)) {
    stop("`field.types` not supported by RPresto", call. = FALSE)
  }
  if (!identical(temporary, FALSE)) {
    stop("Temporary tables not supported by RPresto", call. = FALSE)
  }
  if (!identical(row.names, FALSE)) {
    stop("row.names not supported by RPresto", call. = FALSE)
  }

  sql <- dbplyr::sql_render(
    query = dbplyr::lazy_query(
      query_type = "values", x = value,
      group_vars = character(), order_vars = NULL, frame = NULL
    ),
    con = conn
  )
  dbCreateTableAs(conn, name, sql, overwrite, with, ...)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbWriteTable
#' @export
setMethod(
  "dbWriteTable",
  c("PrestoConnection", "character", "data.frame"),
  .dbWriteTable
)
