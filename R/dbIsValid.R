# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoResult.R
NULL

.dbIsValid <- function(dbObj, ...) {
  return(!dbObj@cursor$state() %in% c('__KILLED', 'FAILED'))
}

#' @rdname PrestoResult-class
#' @export
setMethod('dbIsValid', 'PrestoResult', .dbIsValid)
