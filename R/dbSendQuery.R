# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R PrestoQuery.R
NULL

#' @rdname PrestoConnection-class
#' @importFrom methods new
#' @importMethodsFrom DBI dbSendQuery
#' @export
setMethod(
  "dbSendQuery", c("PrestoConnection", "character"),
  function(conn, statement, ...) {
    query <- PrestoQuery$new(conn, statement, ...)
    result <- query$execute()
    return(result)
  }
)
