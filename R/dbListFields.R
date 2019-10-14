# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

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
    # We cannot use the result object without advancing the cursor.
    # Sometimes presto does not return the full column information, e.g.
    # for the PLANNING state. So we have to kick off a new query.
    new_query <- sprintf("SELECT * FROM (%s) WHERE 1 = 0", conn@statement)
    output <- dbGetQuery(conn@connection, new_query)
    return(colnames(output))
  }
)
