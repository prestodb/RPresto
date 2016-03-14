# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include utility_functions.R PrestoCursor.R
NULL

#' An S4 class to represent a Presto Result
#' @slot post.response The initial response from the HTTP API
#' @slot statement The SQL statement sent to the database
#' @slot session.timezone Session time zone used for the connection
#' @slot cursor An internal implementation detail for keeping track of
#'  what stage a request is in
#' @keywords internal
#' @export
setClass('PrestoResult',
  contains='DBIResult',
  slots=c(
    'post.response'='ANY',
    'statement'='character',
    'session.timezone'='character',
    'cursor'='PrestoCursor'
  )
)

#' @rdname PrestoResult-class
#' @export
setMethod('show',
  'PrestoResult',
  function(object) {
    r <- object@post.response
    content <- response.to.content(r)
    stats <- object@cursor$stats()

    cat(
      '<PrestoResult: ', content[['id']], '>\n',
      'Status Code: ', httr::status_code(r), '\n',
      'State: ', object@cursor$state(), '\n',
      'Info URI: ', object@cursor$infoUri(), '\n',
      'Next URI: ', object@cursor$nextUri(), '\n',
      'Splits (Queued/Running/Completed/Total): ',
      paste(
        c(
          stats[['queuedSplits']],
          stats[['runningSplits']],
          stats[['completedSplits']],
          stats[['totalSplits']]
        ),
        collapse=' / '
      ), '\n',
      'Session Time Zone: ', object@session.timezone, '\n',
      sep=''
    )
  }
)
