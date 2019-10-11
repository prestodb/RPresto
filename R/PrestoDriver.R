# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' An S4 class to represent a Presto Driver (and methods)
#' It is used purely for dispatch and \code{dbUnloadDriver} is unnecessary
#'
#' @keywords internal
#' @export
#' @importFrom methods setClass setGeneric setMethod setRefClass
#' @importFrom methods show getPackageName new
#' @import DBI
setClass('PrestoDriver',
  contains='DBIDriver'
)

#' @rdname PrestoDriver-class
#' @export
setMethod('show',
  'PrestoDriver',
  function(object) {
    cat('<PrestoDriver>\n')
  }
)
