# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

#' dbplyr database methods
#' 
#' @rdname dbplyr-db
#' @param con A `PrestoConnection` as returned by `dbConnect()`.
#' @importFrom dplyr db_list_tables
#' @export
#' @md
db_list_tables.PrestoConnection <- function(con) {
  DBI::dbListTables(con)
}
