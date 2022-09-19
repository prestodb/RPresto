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
setMethod(
  "dbExistsTable",
  c("PrestoConnection", "character"),
  function(conn, name, ...) {
    # This is necessary because name might be a quoted identifier rather than
    # just a string (see #167)
    table_id <- DBI::dbQuoteIdentifier(conn, name)
    table_name <- DBI::dbUnquoteIdentifier(conn, table_id)[[1]]@name
    res <- DBI::dbGetQuery(
      conn,
      paste0("
        SELECT COUNT(*) AS n
        FROM information_schema.columns
        WHERE
          table_catalog = '", conn@catalog, "' AND
          table_schema = '", conn@schema, "' AND
          table_name = '", table_name, "'
      ")
    )
    return((res$n > 0))
  }
)
