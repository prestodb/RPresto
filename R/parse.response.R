# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include utility_functions.R extract.data.R
NULL

.parse.response <- function(result, response) {
  if (!dbIsValid(result)) {
    stop('Result object is not valid')
  }
  df <- data.frame()
  check.status.code(response)
  content <- response.to.content(response)

  if (get.state(content) == 'FAILED') {
    result@cursor$state('FAILED')
    result@cursor$stats(content[['stats']])
    stop.with.error.message(content)
  }

  # Handle SET/RESET SESSION updates
  if (!is.null(content[['updateType']])) {
    switch(
      content[['updateType']],
      'SET SESSION' = {
        properties <- httr::headers(response)[['x-presto-set-session']]
        if (!is.null(properties)) {
          for (pair in strsplit(properties, ',', fixed = TRUE)) {
            pair <- unlist(strsplit(pair, '=', fixed = TRUE))
            result@connection@session$setParameter(pair[1], pair[2])
          }
        }
      },
      'RESET SESSION' = {
        properties <- httr::headers(response)[['x-presto-clear-session']]
        if (!is.null(properties)) {
          for (key in strsplit(properties, ',', fixed = TRUE)) {
            result@connection@session$unsetParameter(key)
          }
        }
      }
    )
  }
  df <- .extract.data(content, timezone=result@connection@session.timezone)
  result@cursor$updateCursor(content, NROW(df))
  return(df)
}
