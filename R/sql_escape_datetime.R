# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' S3 implementation of custom escape method for \link[dbplyr]{sql_escape_datetime}
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dbplyr::sql_escape_datetime,PrestoConnection)
#' } else {
#'   export(sql_escape_datetime.PrestoConnection)
#' }
sql_escape_datetime.PrestoConnection <- function(con, x) {
  # Use unix time to minimize reliance on time zone particulars.
  paste0('FROM_UNIXTIME(', as.numeric(x), ')')
}
