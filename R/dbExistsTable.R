# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbExistsTable
#' @export
setMethod('dbExistsTable',
  c('PrestoConnection', 'character'),
  function(conn, name, ...) {
    # This is necessary because name might be a quoted identifier rather than
    # just a string (see #167)
    name = DBI::dbQuoteIdentifier(conn, name)
    id = DBI::dbUnquoteIdentifier(conn, name)[[1]]@name
    return(tolower(id) %in% tolower(dbListTables(conn, pattern=id)))
  }
)
