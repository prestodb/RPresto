# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbDataType.R
#' @useDynLib RPresto
#' @importFrom Rcpp sourceCpp
NULL

#' Convert a \code{data.frame} formatted in the \code{list} of \code{list}s
#' style as returned by Presto to an actual \code{data.frame}
#'
#' It \sQuote{defines} a few extra \dQuote{data types} to handle all types
#' returned by Presto, like timestamps with time zones.
#'
#' @param data a \code{list} of \code{list}s
#' @param column.types A \code{character} vector of (extended) data types
#'        corresponding to each column. See \code{.presto.to.R} for a list
#'        of possible values.
#' @param timezone The timezone to use for date types.
#' @return A \code{data.frame} of \code{length(data)} rows and
#'         \code{length(data[[1]])} columns.
#'
#' @details If the items in \code{data} are named lists the column names will
#' be inferred from those names. Otherwise they will be
#' \code{paste0('X', seq_along(data[[1]]))}
#'
#' @note For \code{NA} values, \code{data} should have a \code{NULL}
#' item in the correct spot. Ragged arrays are not supported (i.e. all sublists
#' have to have the same length). These \code{NULL} value(s) will be replaced
#' by \code{NA} in-place for the input \code{data}.
#'
#' @rdname json.tabular.to.data.frame
#' @seealso The corresponding unit tests for a full list of capabilities and
#'  data types supported
#' @keywords internal
.json.tabular.to.data.frame <- function(data, column.types, timezone) {
  rv <- NULL
  if (data.class(data) != 'list') {
    stop('Unexpected data class: ', data.class(data))
  }

  # Handle zero column case
  row.count <- length(data)
  column.count <- length(column.types)
  if (column.count == 0) {
    rv <- data.frame()
    if (row.count > 0) {
      attr(rv, 'row.names') <- c(NA, -row.count)
    }
    return(rv)
  }

  rv <- as.list(rep(NA, column.count))
  for (j in seq_along(column.types)) {
    type <- column.types[j]
    rv[[j]] <- switch(type,
      logical=rep(NA, row.count),
      integer=rep(NA_integer_, row.count),
      numeric=rep(NA_real_, row.count),
      character=rep(NA_character_, row.count),
      raw=as.list(rep(NA, row.count)),
      Date=rep(NA_character_, row.count),
      POSIXct_no_time_zone=rep(NA_character_, row.count),
      POSIXct_with_time_zone=rep(NA_character_, row.count),
      list_unnamed=as.list(rep(NA, row.count)),
      list_named=as.list(rep(NA, row.count)),
      stop('Unsupported column type: ', type)
    )
    if (type %in% 'POSIXct_with_time_zone') {
      attr(rv[[j]], 'tzone') <- 'UTC'
    }
  }

  # validate all rows have correct number of column and same column names
  column.names <- check_names(data, column.count)
  # tranpose list from row major to column major
  # NB: null(s) in lists are replaced with NA(s) in-place for efficiency
  transpose(data, rv)

  for (j in which(column.types %in% 'raw')) {
    rv[[j]] <- lapply(rv[[j]], function(txt) {
      if (is.na(txt)) NA else openssl::base64_decode(as.character(txt))
    })
  }

  for (j in which(column.types %in% 'Date')) {
    rv[[j]] <- as.Date(rv[[j]])
  }

  broken.integer.columns <- character(0)
  for (j in which(column.types %in% 'integer')) {
    if (!is.integer(rv[[j]])) {
      broken.integer.columns <- c(broken.integer.columns, j)
    }
  }
  if (length(broken.integer.columns) > 0) {
    warning('integer columns [', paste(broken.integer.columns, collapse=', '),
      '] are cast to double since R does not have full support for ',
      '64-bit integers. This might result in precision loss. ',
      'Cast them to VARCHAR\'s in presto if you need exact values.')
  }

  for (j in which(column.types %in% 'POSIXct_no_time_zone')) {
    rv[[j]] <- as.POSIXct(rv[[j]], tz=timezone)
  }

  for (j in which(column.types %in% 'POSIXct_with_time_zone')) {
    timezones <- stats::na.omit(sub('^.+ ([^ ]+)$', "\\1", rv[[j]], perl=TRUE))
    if (length(unique(timezones)) > 1) {
      warning('Multiple timezones for column ', j, ', ',
        'using ', timezones[1])
    }
    if (length(timezones) > 0) {
      rv[[j]] <- as.POSIXct(
        sub('^(.+) [^ ]+$', "\\1", rv[[j]], perl=TRUE),
        tz=timezones[1]
      )
    } else {
      rv[[j]] <- as.POSIXct(rv[[j]])
      # We have to special case zero and all-NA rows for the following
      # scenario. Assume we have two chunks with 0 and 1 row respectively.
      # rbind will take the timezone from the latter for the resulting
      # data.frame so we will get the expected result. However,
      # dplyr::bind_rows will set it to UTC if the timezones differ. Since
      # we initialize to UTC above, this will lose the correct timezone for
      # the column. See the 'zero chunk first' test in test-dbFetch.
      attr(rv[[j]], 'tzone') <- NULL
    }
  }

  if (is.null(column.names)) {
    column.names <- make.names(character(column.count), unique = TRUE)
  }
  # 'Manually' construct the data.frame. We do it this way for
  # performance purposes. Another option would be to construct the
  # data.frame at the start and assign to each cell one by one.
  # Unfortunately, even with rv[[j]][[i]] indexing, R makes a copy of
  # the data.frame and as such is *very* slow.
  attr(rv, 'class') <- 'data.frame'
  if (row.count > 0) {
    row.names <- c(NA, -1 * row.count)
  } else {
    row.names <- integer(0)
  }
  attr(rv, 'row.names') <- row.names
  colnames(rv) <- column.names
  return(rv)
}
