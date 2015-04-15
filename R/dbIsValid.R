# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include PrestoResult.R
NULL

.dbIsValid <- function(dbObj, ...) {
  return(!dbObj@cursor$state() %in% c('__KILLED', 'FAILED'))
}

#' @rdname PrestoResult-class
#' @export
setMethod('dbIsValid', 'PrestoResult', .dbIsValid)
