# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' S3 implementation of \code{collect} for Presto.
#'
#' @importFrom dplyr collect
#' @export
#' @rdname dplyr_function_implementations
#' @keywords internal
collect.tbl_presto <- function(x, ..., n = Inf, warn_incomplete = TRUE) {
  if (identical(n, Inf)) {
        n <- -1
    }
    else {
        x <- utils::head(x, n)
    }
    sql <- dbplyr::db_sql_render(x$src$con, x)
    # This is the one place whereby this implementation is different from the
    # default dbplyr::collect.tbl_sql()
    # We pass ... to db_collect() here so that bigint can be used in collect()
    # to specify the BIGINT treatment
    out <- dbplyr::db_collect(
      x$src$con, sql, n = n, warn_incomplete = warn_incomplete, ...
    )
    dplyr::grouped_df(out, intersect(dbplyr::op_grps(x), names(out)))
}
