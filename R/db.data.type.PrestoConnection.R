# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' S3 implementation of \code{\link[dplyr]{db_data_type}} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dplyr::db_data_type,PrestoConnection)
#' } else {
#'   export(db_data_type.PrestoConnection)
#' }
db_data_type.PrestoConnection <- function(con, fields, ...) {
  return(sapply(fields, function(field) dbDataType(Presto(), field)))
}
