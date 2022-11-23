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
  signature("PrestoConnection"),
  function(conn, name, ...) {
    table_id <- DBI::dbQuoteIdentifier(conn, name)
    table_name <- DBI::dbUnquoteIdentifier(conn, table_id)[[1]]@name
    res <- DBI::dbGetQuery(
      conn,
      paste0("
        SELECT COUNT(*) AS n
        FROM information_schema.columns
        WHERE
          table_catalog = '", conn@catalog, "' AND
          table_schema = '", ifelse(!"schema" %in% names(table_name), conn@schema, table_name["schema"]), "' AND
          table_name = '", table_name["table"], "'
      ")
    )
    return((res$n > 0))
  }
)
