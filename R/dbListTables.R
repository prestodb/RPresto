# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R dbGetQuery.R
NULL

#' @rdname PrestoConnection-class
#' @param pattern optional SQL pattern for filtering table names,
#'                e.g. \sQuote{\%test\%}
#' @export
setMethod('dbListTables',
  'PrestoConnection',
  function(conn, pattern, ...) {
    if (!missing(pattern)) {
      statement <- paste('SHOW TABLES LIKE', dbQuoteString(conn, pattern))
    } else {
      statement <- 'SHOW TABLES'
    }
    rv <- dbGetQuery(conn, statement)
    if (nrow(rv)) {
      return(rv[['Table']])
    } else {
      return(character(0))
    }
  }
)
