# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include src.translate.env.src.presto.R
NULL

#' S3 implementation of \code{sql_translate_env} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#' @export
sql_translate_env.PrestoConnection <- function(conn) {
  return(src_translate_env.src_presto(conn))
}
