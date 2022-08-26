# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

.dbGetQuery <- function(conn, statement, ...) {
  result <- dbSendQuery(conn, statement, ...)
  on.exit(dbClearResult(result))
  df <- dbFetch(result, -1)
  return(df)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbGetQuery
#' @export
setMethod('dbGetQuery', c('PrestoConnection', 'character'), .dbGetQuery)
