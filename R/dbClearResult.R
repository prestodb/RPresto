# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbHasCompleted.R PrestoResult.R request_headers.R
NULL

#' @rdname PrestoResult-class
#' @export
setMethod('dbClearResult',
  c('PrestoResult'),
  function(res, ...) {
    if (dbHasCompleted(res)) {
      return(TRUE)
    }

    uri <- res@cursor$nextUri()
    if (uri == '') {
      return(TRUE)
    }

    if (res@cursor$state() == '__KILLED') {
      return(TRUE)
    }

    headers <- .request_headers(res@connection)
    delete.result <- httr::DELETE(uri, config=headers)
    s <- httr::status_code(delete.result)
    if (s >= 200 && s < 300) {
      res@cursor$state('__KILLED')
      rv <- TRUE
    } else {
      rv <- FALSE
    }
    return(rv)
  }
)
