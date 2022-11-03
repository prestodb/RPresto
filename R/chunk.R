# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' Add a chunk field to a data frame
#'
#' This auxiliary function adds a field, if necessary, to a data frame so that
#' each compartment of the data frame that corresponds to a unique combination
#' of the chunk fields has a size below a certain threshold. This resulting
#' data frame can then be safely used in dbAppendTable() becauase Presto has a
#' size limit on any discrete INSERT INTO statement.
#'
#' @param value The original data frame.
#' @param chunk_size Maximum size (in bytes) of each unique chunk. Default to
#'   750,000 bytes.
#' @param chunk_fields A character vector of existing field names that are used
#'   to split the data frame.
#' @param new_chunk_field_name A string indicating the new chunk field name.
#'   Default to "chunk".
#' @importFrom rlang :=
#' @export
#' @examples
#' \dontrun{
#' # returns the original data frame because it's within size
#' add_chunk(iris)
#' # add a new chunk_idx field
#' add_chunk(iris, chunk_size = 2000)
#' # the new chunk_idx field is added on top of Species
#' add_chunk(iris, chunk_size = 2000, chunk_fields = c("Species"))
#' }
add_chunk <- function(
  value, chunk_size = 7.5e5,
  chunk_fields = NULL, new_chunk_field_name = "chunk_idx"
) {
  .add_chunk <- function(value, start = 1L) {
    if (new_chunk_field_name %in% colnames(value)) {
      stop(
        paste0(new_chunk_field_name, " is already found in the data frame."),
        call. = FALSE
      )
    }
    n_chunks <- (as.integer(utils::object.size(value)) %/% chunk_size) + 1
    chunk_size <- nrow(value) %/% n_chunks
    dplyr::mutate(
      dplyr::ungroup(value),
      !!rlang::sym(new_chunk_field_name) :=
        start + as.integer((dplyr::row_number() - 1L) %/% chunk_size)
    )
  }

  if (!is.null(chunk_fields)) {
    split_values <- dplyr::group_split(value, !!!rlang::syms(chunk_fields))
    start <- 0L
    res <- vector(mode = "list", length = length(split_values))
    for (i in seq_along(res)) {
      res[[i]] <- .add_chunk(split_values[[i]], start = start + 1)
      start <-
        max(dplyr::pull(res[[i]], !!rlang::sym(new_chunk_field_name)))
    }
    if (length(split_values) == start) {
      return(value)
    } else {
      return(dplyr::bind_rows(res))
    }
  } else {
    if (utils::object.size(value) <= chunk_size) {
      return(value)
    } else {
      return(.add_chunk(value))
    }
  }
}
