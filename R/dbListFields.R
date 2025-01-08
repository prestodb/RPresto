# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R PrestoResult.R
NULL

#' @rdname PrestoConnection-class
#' @inheritParams DBI::dbListFields
#' @param conn a `PrestoConnection` object, as returned by [DBI::dbConnect()].
#' @usage NULL
.dbListFields_PrestoConnection <- function(conn, name, ...) {
    name <- DBI::dbQuoteIdentifier(conn, name)
    res <- dbGetQuery(conn, paste("SHOW COLUMNS FROM", name))
    return(res$Column)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbListFields
#' @export
setMethod("dbListFields", signature("PrestoConnection"), .dbListFields_PrestoConnection)

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbListFields
#' @export
setMethod("dbListFields", signature("PrestoConnection", "character"), .dbListFields_PrestoConnection)

setOldClass("dbplyr_schema")

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbListFields
#' @export
setMethod("dbListFields", signature("PrestoConnection", "dbplyr_schema"), .dbListFields_PrestoConnection)

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbListFields
#' @export
setMethod("dbListFields", signature("PrestoConnection", "Id"), .dbListFields_PrestoConnection)

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbListFields
#' @export
setMethod("dbListFields", signature("PrestoConnection", "SQL"), .dbListFields_PrestoConnection)

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbListFields
#' @export
setMethod(
  "dbListFields",
  signature(conn = "PrestoResult", name = "missing"),
  function(conn, name) {
    if (!dbIsValid(conn)) {
      stop("The result object is not valid")
    }
    # We cannot use the result object without advancing the query.
    # Sometimes presto does not return the full column information, e.g.
    # for the PLANNING state. So we have to kick off a new query.
    new_query <- sprintf("SELECT * FROM (%s) WHERE 1 = 0", conn@statement)
    output <- dbGetQuery(conn@connection, new_query)
    return(colnames(output))
  }
)
