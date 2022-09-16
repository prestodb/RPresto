# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoDriver.R
NULL

#' Connect to a Presto database
#' @return [Presto] A [PrestoDriver-class] object
#' @rdname Presto
#' @export
Presto <- function(...) {
  return(methods::new("PrestoDriver"))
}
