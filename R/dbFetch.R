# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include extract.data.R json.tabular.to.data.frame.R
#' @include utility_functions.R PrestoResult.R parse.response.R
#' @include request_headers.R
NULL

.fetch.uri.with.retries <- function(uri, headers, num.retry=3) {
  get.response <- tryCatch({
      response <- httr::GET(uri, config=headers)
      if (httr::status_code(response) >= 400L) {
        # stop_for_status also fails for 300 <= status < 400
        # so we need the if condition
        httr::stop_for_status(response)
      }
      response
    },
    error=function (e) {
      if (num.retry == 0) {
        stop("There was a problem with the request ",
         "and we have exhausted our retry limit for uri: ", uri)
      }
      message('GET call failed with error: "', conditionMessage(e),
              '", retrying [', 4 - num.retry, '/3]\n')
      wait()
      httr::handle_reset(uri)
      return(.fetch.uri.with.retries(uri, headers, num.retry - 1))
    }
  )
  return(get.response)
}

.fetch.single.uri <- function(res, n, ...) {
  if (!dbIsValid(res)) {
    stop('Result object is not valid')
  }
  df <- data.frame()
  headers <- .request_headers(res@connection)
  if (!res@cursor$hasCompleted()) {
    get.response <- tryCatch(
      .fetch.uri.with.retries(res@cursor$nextUri(), headers=headers),
      error=function(e) {
        res@cursor$state('FAILED')
        stop(simpleError(
          paste0(
            'Cannot fetch ', res@cursor$nextUri(), ', ',
            'error: ', conditionMessage(e)
          ),
          call='.fetch.single.uri'
        ))
      }
    )
    df <- .parse.response(res, get.response)
  }
  return(df)
}

.fetch.all <- function(result) {
  rv <- list()
  chunk.count <- 1
  while (!dbHasCompleted(result)) {
    chunk <- dbFetch(result)
    rv[[chunk.count]] <- chunk
    chunk.count <- chunk.count + 1
  }
  if (length(rv) == 1) {
    # Preserve attributes for empty data frames
    return(rv[[1]])
  } else {
    # We need to check for the uniqueness of columns because dplyr::bind_rows
    # will drop duplicate column names and we want to preserve all the data
    unique.chunk.column.names <- unique(lapply(
      Filter(function(df) NROW(df) || NCOL(df), rv),
      names
    ))
    if (length(unique.chunk.column.names) != 1) {
      stop('Chunk column names are different across chunks: ',
        jsonlite::toJSON(lapply(rv, names))
      )
    }
    column.names <- unique.chunk.column.names[[1]]
    if (
      requireNamespace('dplyr', quietly=TRUE) &&
      length(column.names) == length(unique(column.names))
    ) {
      return(as.data.frame(dplyr::bind_rows(rv)))
    } else {
      return(do.call('rbind', rv))
    }
  }
}

.fetch.with.count <- function(res, n, ...) {
  if (!dbIsValid(res)) {
    stop('Result object is not valid')
  }
  if (!((n > 0 && is.infinite(n))
      || (as.integer(n) == -1L))) {
    stop('fetching custom number of rows (n != -1 and n != Inf) ',
         'is not supported, asking for: ', n)
  }
  return(.fetch.all(res))
}

.fetch.next.chunk <- function(res, n, ...) {
  if (!res@cursor$postDataParsed()) {
    df <- .parse.response(res, res@cursor$postResponse())
    res@cursor$postDataParsed(TRUE)
    return(df)
  }
  return(.fetch.single.uri(res, n, ...))
}

#' @rdname PrestoResult-class
#' @export
setMethod('dbFetch', c('PrestoResult', 'integer'), .fetch.with.count)

#' @rdname PrestoResult-class
#' @export
setMethod('dbFetch', c('PrestoResult', 'numeric'), .fetch.with.count)

#' @rdname PrestoResult-class
#' @export
setMethod('dbFetch', c('PrestoResult', 'missing'), .fetch.next.chunk)
