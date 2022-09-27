# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

.onLoad <- function(libname, pkgname) {
  # Default options --------------------------------------------------------
  op <- options()
  defaults <- list(
    rpresto.max.rows = 1000000L,
    rpresto.quiet = NA
  )
  toset <- !(names(defaults) %in% names(op))
  if (any(toset)) options(defaults[toset])

  invisible()
}
