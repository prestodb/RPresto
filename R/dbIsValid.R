# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoResult.R
NULL

.dbIsValid <- function(dbObj, ...) {
  return(!dbObj@query$state() %in% c("__KILLED", "FAILED"))
}

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbIsValid
#' @export
setMethod("dbIsValid", "PrestoResult", .dbIsValid)
