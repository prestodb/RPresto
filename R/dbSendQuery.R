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
#' @param quiet If a progress bar should be shown for long queries (which run
#'        for more than 2 seconds. Default to `getOption("rpresto.quiet")` which
#'        if not set, defaults to `NA` which turns on the progress bar for
#'        interactive queries.
#' @export
setMethod(
  "dbSendQuery", c("PrestoConnection", "character"),
  function(conn, statement, ..., quiet = getOption("rpresto.quiet")) {
    query <- PrestoQuery$new(conn, statement, quiet = quiet, ...)
    result <- query$execute()
    return(result)
  }
)
