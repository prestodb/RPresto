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
.dbAppendTable <- function(conn, name, value, ...) {
  is_factor <- vapply(value, is.factor, logical(1L))
  if (any(is_factor)) {
    value[is_factor] <- lapply(value[is_factor], as.character)
  }
  chunks <- (as.integer(object.size(value)) %/% 7.5e5) + 1
  if (chunks >= 100) {
    stop("The value to append is too large.", call. = FALSE)
  }
  chunk_size <- nrow(value) %/% chunks
  total_rows <- 0L
  pb <- progress::progress_bar$new(
    format = "  appending chunk #:chunk [:bar] :percent in :elapsed",
    clear = FALSE,
    total = 100,
    width = 80,
    show_after = 1
  )
  pb$tick(0, tokens = list(chunk = 0L))
  for (i in 1:chunks) {
    start <- (i-1)*chunk_size+1
    end <- min(i*chunk_size, nrow(value))
    sql <- DBI::sqlAppendTable(
      conn, name, dplyr::slice(value, start:end), row.names = FALSE
    )
    rows <- DBI::dbExecute(conn, sql, quiet = TRUE)
    pb$update(ratio = i/chunks, tokens = list(chunk = i))
    total_rows <- total_rows + rows
  }
  return(total_rows)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbAppendTable
#' @export
setMethod(
  "dbAppendTable",
  c("PrestoConnection", "character", "data.frame"),
  .dbAppendTable
)
