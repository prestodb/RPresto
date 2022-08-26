# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' S3 implementation of \code{\link[dplyr]{copy_to}} for Presto.
#'
#' @importFrom dplyr copy_to
#' @export
#' @rdname dplyr_source_function_implementations
#' @keywords internal
copy_to.src_presto <- function(dest, df) {
  stop("Not implemented.")
}
