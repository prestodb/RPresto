# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include dbplyr_compatible.R src.presto.R
NULL

#' dplyr integration to connect to a table in a database.
#'
#' Use \code{src_presto} to connect to an existing database,
#' and \code{tbl} to connect to tables within that database.
#' If you're unsure of the arguments to pass, please ask your database
#' administrator for the values of these variables.
#'
#' @param src A presto src created with \code{src_presto}.
#' @param from Either a string giving the name of table in database, or
#'   \code{\link[dplyr]{sql}} described a derived table or compound join.
#' @examples
#' \dontrun{
#' First create a database connection with src_presto, then reference a tbl
#' within that database
#' my_tbl <- tbl(my_db, "my_table")
#' }
#' @rdname src_presto
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dplyr::tbl,src_presto)
#' } else {
#'   export(tbl.src_presto)
#' }
tbl.src_presto <- function(src, from, ...) {
  tbl_sql <- dbplyr_compatible('tbl_sql')
  rv <- tbl_sql("presto", src = src, from = from, ...)
  if (!inherits(rv, 'tbl_presto')) {
    class(rv) <- c('tbl_presto', class(rv))
  }
  return(rv)
}
