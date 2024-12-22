# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoResult.R
NULL

.combine_results <- function(res_list) {
  if (length(res_list) == 1) {
    # Preserve attributes for empty data frames
    return(res_list[[1]])
  } else {
    res_list <- purrr::keep(res_list, ~ (ncol(.) > 0))
    # We need to check for the uniqueness of columns because dplyr::bind_rows
    # will drop duplicate column names and we want to preserve all the data
    unique.chunk.column.names <- unique(lapply(
      Filter(function(df) NROW(df) || NCOL(df), res_list),
      names
    ))
    if (length(unique.chunk.column.names) > 1) {
      stop(
        "Chunk column names are different across chunks: ",
        jsonlite::toJSON(lapply(res_list, names))
      )
    }
    return(dplyr::bind_rows(res_list))
  }
}

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbFetch
#' @export
setMethod(
  "dbFetch", c("PrestoResult", "numeric"),
  function(res, n) {
    if (!dbIsValid(res)) {
      stop("Result object is not valid")
    }
    if (!((n > 0 && is.infinite(n)) || (as.integer(n) == -1L))) {
      stop(
        "fetching custom number of rows (n != -1 and n != Inf) ",
        "is not supported, asking for: ", n
      )
    }
    res_list <- list()
    chunk.count <- 1
    while (!dbHasCompleted(res)) {
      chunk <- dbFetch(res)
      res_list[[chunk.count]] <- chunk
      chunk.count <- chunk.count + 1
    }
    if (res@query$.progress$finished) {
      res@query$.progress$terminate()
    }
    max_rows <- getOption("rpresto.max.rows")
    if (res@query$fetchedRowCount() >= max_rows) {
      warning(
        "You have loaded more rows than the maximum (", max_rows, ") into ",
        "memory. ", "Please make sure that you're not fetching too much data. ",
        "Consider sampling or aggregation before you return all the data. ",
        "You can change the maximum rows using options(\"rpresto.max.rows\").",
        call. = FALSE, immediate. = TRUE
      )
    }
    df <- .combine_results(res_list)
    return(df)
  }
)

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbFetch
#' @export
setMethod(
  "dbFetch", c("PrestoResult", "missing"),
  function(res) {
    if (!dbIsValid(res)) {
      stop("Result object is not valid")
    }
    if (isFALSE(res@query$postDataFetched())) {
      df <- res@post.data
      res@query$postDataFetched(TRUE)
    } else {
      df <- res@query$fetch()
    }
    res@query$fetchedRowCount(NROW(df))
    return(df)
  }
)
