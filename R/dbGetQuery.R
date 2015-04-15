# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbFetch.R dbSendQuery.R dbHasCompleted.R PrestoConnection.R
NULL

.dbGetQuery <- function(conn, statement, ...) {
  result <- dbSendQuery(conn, statement, ...)
  on.exit(dbClearResult(result))
  return(.fetch.all(result))
}

#' @rdname PrestoConnection-class
#' @export
setMethod('dbGetQuery', c('PrestoConnection', 'character'), .dbGetQuery)
