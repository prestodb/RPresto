# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' An S4 class to represent a Presto Driver (and methods)
#' It is used purely for dispatch and `dbUnloadDriver` is unnecessary
#'
#' @keywords internal
#' @importClassesFrom DBI DBIDriver
#' @export
setClass("PrestoDriver",
  contains = "DBIDriver"
)

#' @rdname PrestoDriver-class
#' @importMethodsFrom methods show
#' @export
setMethod(
  "show",
  "PrestoDriver",
  function(object) {
    cat("<PrestoDriver>\n")
  }
)
