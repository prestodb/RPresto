# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R PrestoResult.R dbColumnType.R
NULL

#' @rdname PrestoResult-class
#' @inheritParams DBI::dbColumnInfo
#' @param res A PrestoResult object
#' @return A data frame with columns:
#'   - name: Column name (character)
#'   - type: R type (character)
#'   - .presto_type: Presto type as string (character)
#' @importMethodsFrom DBI dbColumnInfo
.dbColumnInfo_PrestoResult <- function(res, ...) {
  # Use the shared helper function from dbColumnType.R
  return(.get_column_info_from_result(res))
}

#' @rdname PrestoResult-class
#' @importMethodsFrom DBI dbColumnInfo
#' @export
setMethod("dbColumnInfo", "PrestoResult", .dbColumnInfo_PrestoResult)

