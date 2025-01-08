# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' @param conn A [PrestoConnection-class] object
#' @return [DBI::dbDisconnect()] A [logical()] value indicating success
#' @importMethodsFrom DBI dbDisconnect
#' @export
#' @rdname Presto
setMethod(
  "dbDisconnect",
  "PrestoConnection",
  function(conn) {
    return(TRUE)
  }
)
