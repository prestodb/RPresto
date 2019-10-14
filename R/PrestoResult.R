# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include utility_functions.R PrestoCursor.R PrestoConnection.R
NULL

#' An S4 class to represent a Presto Result
#' @slot statement The SQL statement sent to the database
#' @slot connection The connection object associated with the result
#' @slot cursor An internal implementation detail for keeping track of
#'  what stage a request is in
#' @keywords internal
#' @export
setClass('PrestoResult',
  contains='DBIResult',
  slots=c(
    'statement'='character',
    'connection'='PrestoConnection',
    'cursor'='PrestoCursor'
  )
)

#' @rdname PrestoResult-class
#' @export
setMethod('show',
  'PrestoResult',
  function(object) {
    r <- object@cursor$postResponse()
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
      'Session Time Zone: ', object@connection@session.timezone, '\n',
      sep=''
    )
  }
)
