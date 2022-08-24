# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

.description_from_info <- function(info) {
  return(paste0(
    'presto ',
    ' [',
    info[['schema']],
    ':',
    info[['catalog']],
    ' | ',
    info[['user']],
    '@',
    info[['host']],
    ':',
    info[['port']],
    ']'
  ))
}

#' S3 implementation of \code{db_desc} for Presto.
#'
#' @importFrom dplyr db_desc
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
db_desc.PrestoConnection <- function(x) {
  info <- dbGetInfo(x)
  return(.description_from_info(info))
}

