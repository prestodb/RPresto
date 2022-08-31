# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' S3 implementation of custom escape method for \link[dbplyr]{sql_escape_date}
#'
#' @importFrom dbplyr sql_escape_date
#' @export
#' @rdname dbplyr_function_implementations
#' @keywords internal
sql_escape_date.PrestoConnection <- function(con, x) {
  paste0('DATE ', DBI::dbQuoteString(con, as.character(x)))
}
