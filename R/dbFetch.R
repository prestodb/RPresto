# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include extract.data.R json.tabular.to.data.frame.R
#' @include utility_functions.R PrestoResult.R
NULL

.fetch.uri.with.retries <- function(uri, num.retry=3) {
  get.response <- tryCatch({
      httr::GET(uri)
    },
    error=function (e) {
      if (num.retry == 0) {
        stop("There was a problem with the request ",
         "and we have exhausted our retry limit")
      }
      message('GET call failed with error: "', conditionMessage(e),
              '", retrying [', 4 - num.retry, '/3]\n')
      wait()
      httr::handle_reset(uri)
      return(.fetch.uri.with.retries(uri, num.retry - 1))
    }
  )
  return(get.response)
}

.fetch.single.uri <- function(res, n, ...) {
  if (!dbIsValid(res)) {
    stop('Result object is not valid')
  }
  df <- data.frame()
  if (!res@cursor$hasCompleted()) {
    get.response <- tryCatch(
      .fetch.uri.with.retries(res@cursor$nextUri()),
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

    check.status.code(get.response)
    content <- response.to.content(get.response)
    if (get.state(content) == 'FAILED') {
      res@cursor$state('FAILED')
      res@cursor$stats(content[['stats']])
      stop.with.error.message(content)
    }
    df <- .extract.data(content, timezone=res@session.timezone)
    res@cursor$updateCursor(content, NROW(df))
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
    if (requireNamespace('dplyr', quietly=TRUE)) {
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
  if (as.integer(n) != -1L) {
    stop('fetching custom number of rows (n != -1) is not supported.')
  }
  return(.fetch.all(res))
}

#' @rdname PrestoResult-class
#' @export
setMethod('dbFetch', c('PrestoResult', 'integer'), .fetch.with.count)

#' @rdname PrestoResult-class
#' @export
setMethod('dbFetch', c('PrestoResult', 'numeric'), .fetch.with.count)

#' @rdname PrestoResult-class
#' @export
setMethod('dbFetch', c('PrestoResult', 'missing'), .fetch.single.uri)
