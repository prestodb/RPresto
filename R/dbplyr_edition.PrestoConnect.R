# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' Inform the dbplyr version used in this package
#'
#' @importFrom dbplyr dbplyr_edition
#' @export
#' @param con A DBIConnection object.
dbplyr_edition.PrestoConnection <- function(con) 2L
