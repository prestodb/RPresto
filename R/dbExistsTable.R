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
    table_name <- DBI::dbQuoteIdentifier(conn, name)
    tryCatch(
      {
        res <- dplyr::db_query_fields(conn, table_name)
        return(TRUE)
      },
      error = function(e) {
        if (grepl("Table .* does not exist", conditionMessage(e))) {
          return(FALSE)
        } else {
          stop("Cannot tell if ", name, " exists.", call. = FALSE)
        }
      }
    )
  }
)
