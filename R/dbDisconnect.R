# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

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
