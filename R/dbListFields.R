# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbGetQuery.R PrestoConnection.R PrestoResult.R utility_functions.R
NULL

#' @rdname PrestoConnection-class
#' @export
setMethod('dbListFields',
  c('PrestoConnection', 'character'),
  function(conn, name, ...) {
    quoted.name <- dbQuoteIdentifier(conn, name)
    names(dbGetQuery(conn, paste('SELECT * FROM', quoted.name, 'LIMIT 0')))
  }
)

#' @rdname PrestoResult-class
#' @export
setMethod('dbListFields',
  signature(conn='PrestoResult', name='missing'),
  function(conn, name) {
    if (!dbIsValid(conn)) {
      stop('The result object is not valid')
    }
    info.response <- .fetch.uri.with.retries(conn@cursor$infoUri())
    check.status.code(info.response)
    info <- response.to.content(info.response)
    if (!is.null(info[['failureInfo']])
        && !is.null(info[['failureInfo']][['message']])
    ) {
      stop('Query failed: ', info[['failureInfo']][['message']])
    }
    if (!is.null(info[['fieldNames']])) {
      rv <- unlist(info[['fieldNames']])
    } else {
      rv <- character(0)
    }
    return(rv)
  }
)
