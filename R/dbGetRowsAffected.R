# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoResult.R
NULL

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbGetRowsAffected
#' @export
setMethod(
  "dbGetRowsAffected", "PrestoResult",
  function(res) {
    df <- dbFetch(res, -1)
    if ("rows" %in% colnames(df)) {
      return(as.integer(df$rows))
    }
    return(0L)
  }
)
