# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoResult.R
NULL

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbClearResult
#' @export
setMethod(
  "dbClearResult",
  c("PrestoResult"),
  function(res, ...) {
    if (dbHasCompleted(res)) {
      return(TRUE)
    }

    uri <- res@query$nextUri()
    if (uri == "") {
      return(TRUE)
    }

    if (res@query$state() == "__KILLED") {
      return(TRUE)
    }

    headers <- .request_headers(res@connection)
    delete.uri <- paste0(
      res@connection@host, ":", res@connection@port,
      "/v1/query/", res@query$id()
    )
    delete.result <- httr::DELETE(delete.uri, config = headers)
    s <- httr::status_code(delete.result)
    if (s >= 200 && s < 300) {
      res@query$state("__KILLED")
      rv <- TRUE
    } else {
      rv <- FALSE
    }
    return(rv)
  }
)
