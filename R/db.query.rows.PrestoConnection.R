# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' S3 implementation of \code{\link[dplyr]{db_query_rows}} for Presto.
#'
#' @importFrom dplyr db_query_rows
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_query_rows.PrestoConnection <- function(con, sql) {
  # We shouldn't be doing a COUNT(*) over arbitrary tables because Hive tables
  # can be prohibitively long. There may be something smarter we can do for
  # smaller tables though.
  return(NA)
}

