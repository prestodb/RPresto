# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbQuoteIdentifier
#' @usage NULL
.dbQuoteIdentifier_PrestoConnection_dbplyr_schema <- function(conn, x, ...) {
  x_id <- DBI::Id(
    schema = as.character(x$schema),
    table = as.character(x$table)
  )
  DBI::dbQuoteIdentifier(conn, x_id)
}

setOldClass("dbplyr_schema")

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbQuoteIdentifier
#' @export
setMethod(
  "dbQuoteIdentifier",
  signature("PrestoConnection", "dbplyr_schema"),
  .dbQuoteIdentifier_PrestoConnection_dbplyr_schema
)
