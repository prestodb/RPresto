# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @usage NULL
.dbReadTable <- function(conn, name, ...) {
  name <- DBI::dbQuoteIdentifier(conn, name)
  out <- DBI::dbGetQuery(
    conn, paste0("SELECT * FROM ", name), ...
  )
  return(out)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbReadTable
#' @export
setMethod("dbReadTable", c("PrestoConnection", "character"), .dbReadTable)
