# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include src.presto.R
NULL

#' S3 implementation of \code{\link[dplyr]{copy_to}} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dplyr::copy_to,src_presto)
#' } else {
#'   export(copy_to.src_presto)
#' }
copy_to.src_presto <- function(dest, df) {
  stop("Not implemented.")
}
