# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include PrestoConnection.R
NULL

#' @param conn A \code{\linkS4class{PrestoConnection}} object
#' @return [dbDisconnect] A \code{\link{logical}} value indicating success
#' @export
#' @rdname Presto
setMethod('dbDisconnect',
  'PrestoConnection',
  function(conn) {
    return(TRUE)
  }
)
