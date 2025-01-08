# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbQuoteIdentifier
#' @param conn a `PrestoConnection` object, as returned by [DBI::dbConnect()].
#' @usage NULL
.dbQuoteIdentifier_PrestoConnection_dbplyr_schema <- function(conn, x, ...) {
  x_id <- DBI::Id(
    schema = as.character(x$schema),
    table = as.character(x$table)
  )
  DBI::dbQuoteIdentifier(conn, x_id, ...)
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

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbQuoteIdentifier
#' @usage NULL
.dbQuoteIdentifier_PrestoConnection_dbplyr_table_path <- function(conn, x, ...) {
  x_components <- dbplyr::table_path_components(x, conn)[[1]]
  if (length(x_components) == 3L) {
    if (x_components[1] != conn@catalog) {
      stop("Mismatched catalogs", call. = FALSE)
    }
    x_components <- x_components[2L:3L]
  }
  if (length(x_components) == 2L) {
    x_id <- DBI::Id(
      schema = as.character(x_components[1]),
      table = as.character(x_components[2])
    )
    x_qid <- DBI::dbQuoteIdentifier(conn, x_id, ...)
  }
  if (length(x_components) == 1L) {
    x_qid <- DBI::dbQuoteIdentifier(conn, x_components, ...)
  }
  return(x_qid)
}

setOldClass("dbplyr_table_path")

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbQuoteIdentifier
#' @export
setMethod(
  "dbQuoteIdentifier",
  signature("PrestoConnection", "dbplyr_table_path"),
  .dbQuoteIdentifier_PrestoConnection_dbplyr_table_path
)

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbQuoteIdentifier
#' @usage NULL
.dbQuoteIdentifier_PrestoConnection_AsIs <- function(conn, x, ...) {
  if (utils::packageVersion("dbplyr") < "2.5.0") {
    stop("Using I() in table name required dbplyr >= 2.5.0", call. = FALSE)
  }
  x_tp <- dbplyr::as_table_path(x, conn)
  dbQuoteIdentifier(conn, x_tp, ...)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbQuoteIdentifier
#' @export
setMethod(
  "dbQuoteIdentifier",
  signature("PrestoConnection", "AsIs"),
  .dbQuoteIdentifier_PrestoConnection_AsIs
)
