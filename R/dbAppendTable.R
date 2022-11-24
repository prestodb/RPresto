# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbAppendTable
#' @usage NULL
.dbAppendTable <- function(
  conn, name, value, ..., chunk.fields = NULL, row.names = NULL
) {
  name <- DBI::dbQuoteIdentifier(conn, name)
  is_factor <- vapply(value, is.factor, logical(1L))
  if (any(is_factor)) {
    value[is_factor] <- lapply(value[is_factor], as.character)
  }
  if (!is.null(chunk.fields)) {
    is_chunk_field_found <- chunk.fields %in% colnames(value)
    if (!all(is_chunk_field_found)) {
      stop(
        "Fields [",
        paste(chunk.fields[!is_chunk_field_found], collapse = ", "),
        "] are not found.",
        call. = FALSE
      )
    }
    chunk_value <- dplyr::group_split(value, !!!rlang::syms(chunk.fields))
    n_chunks <- length(chunk_value)
    message("\n", n_chunks, " chunks are found and to be inserted.")
    total_rows <- 0L
    pb <- progress::progress_bar$new(
      format = "  appending chunk #:chunk [:bar] :percent in :elapsed",
      clear = FALSE,
      total = 100,
      width = 80,
      show_after = 1
    )
    pb$tick(0, tokens = list(chunk = 0L))
    for (i in 1:n_chunks) {
      sql <- DBI::sqlAppendTable(
        conn, name, chunk_value[[i]], row.names = row.names
      )
      rows <- DBI::dbExecute(conn, sql, quiet = TRUE)
      pb$update(ratio = i/n_chunks, tokens = list(chunk = i))
      total_rows <- total_rows + rows
    }
  } else {
    sql <- DBI::sqlAppendTable(conn, name, value, row.names = row.names)
    total_rows <- DBI::dbExecute(conn, sql)
  }
  return(total_rows)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbAppendTable
#' @export
setMethod(
  "dbAppendTable",
  signature("PrestoConnection", "ANY", "data.frame"),
  .dbAppendTable
)
