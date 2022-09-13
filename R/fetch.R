# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoResult.R
NULL

.defer.to.dbFetch <- function(res, n, ...) {
  return(dbFetch(res, n, ...))
}

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI fetch
#' @export
setMethod("fetch", c("PrestoResult", "integer"), .defer.to.dbFetch)

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI fetch
#' @export
setMethod("fetch", c("PrestoResult", "numeric"), .defer.to.dbFetch)

# due to the way generics are set, if we do not do this override the default
# value from the generic n=-1 gets set which could give wrong results
#' @rdname PrestoResult-class
#' @importMethodsFrom DBI fetch
#' @export
setMethod(
  "fetch",
  c("PrestoResult", "missing"),
  function(res) {
    return(dbFetch(res))
  }
)
