# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' S3 implementation of custom escape method for \link[dbplyr]{sql_escape_date}
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dbplyr::sql_escape_date,PrestoConnection)
#' } else {
#'   export(sql_escape_date.PrestoConnection)
#' }
sql_escape_date.PrestoConnection <- function(con, x) {
  paste0('DATE ', dbQuoteString(con, as.character(x)))
}
