# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoDriver.R
NULL

get_vector_type <- function(obj) {
  if (is.factor(obj)) return("VARCHAR")
  if (inherits(obj, "POSIXct")) return("TIMESTAMP")
  if (inherits(obj, "Date")) return("DATE")
  if (inherits(obj, "difftime")) return("TIME")
  if (inherits(obj, "integer64")) return("BIGINT")
  switch(typeof(obj),
    integer = "INTEGER",
    double = "DOUBLE",
    character = "VARCHAR",
    logical = "BOOLEAN",
    raw = "VARBINARY",
    NULL = "VARCHAR",
    stop("Unsupported type", call. = FALSE)
  )
}

.dbDataType <- function(dbObj, obj, ...) {
  if (is.data.frame(obj)) return(vapply(obj, dbDataType, "", dbObj = dbObj))
  if (is.list(obj)) {
    element_types <- vapply(obj, dbDataType, "", dbObj = dbObj)
    if (length(unique(element_types)) == 1L) {
      if (all(purrr::map_lgl(obj, ~is.null(names(.))))) {
        return(paste0("ARRAY<", unique(element_types), ">"))
      } else {
        return(paste0("MAP<VARCHAR, ", unique(element_types), ">"))
      }
    } else {
      stop("Unsupported list type", call. = FALSE)
    }
  }
  get_vector_type(obj)
}

#' Return the corresponding presto data type for the given R `object`
#' @param dbObj A [PrestoDriver-class] object
#' @param obj Any R object
#' @param ... Extra optional parameters, not currently used
#' @return A `character` value corresponding to the Presto type for
#'         `obj`
#' @rdname dbDataType
#' @details The default value for unknown classes is \sQuote{VARCHAR}.
#'
#' @examples
#' drv <- RPresto::Presto()
#' dbDataType(drv, 1)
#' dbDataType(drv, NULL)
#' dbDataType(drv, as.POSIXct("2015-03-01 00:00:00", tz = "UTC"))
#' dbDataType(drv, Sys.time())
#' dbDataType(
#'   drv,
#'   list(
#'     c("a" = 1L, "b" = 2L),
#'     c("a" = 3L, "b" = 4L)
#'   )
#' )
#' dbDataType(
#'   drv,
#'   list(
#'     c(as.Date("2015-03-01"), as.Date("2015-03-02")),
#'     c(as.Date("2016-03-01"), as.Date("2016-03-02"))
#'   )
#' )
#' dbDataType(drv, iris)
#' @importMethodsFrom DBI dbDataType
#' @export
setMethod("dbDataType", "PrestoDriver", .dbDataType)
