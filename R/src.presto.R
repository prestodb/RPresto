# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' dplyr integration to connect to a Presto database.
#'
#' Allows you to connect to an existing database through a presto connection.
#'
#' @param catalog Catalog to use in the connection
#' @param schema Schema to use in the connection
#' @param user User name to use in the connection
#' @param host Host name to connect to the database
#' @param port Port number to use with the host name
#' @param session.timezone Time zone for the connection
#' @param parameters Additional parameters to pass to the connection
#' @param ... For \code{src_presto} other arguments passed on to the underlying
#'   database connector \code{dbConnect}. For \code{tbl.src_presto}, it is
#'   included for compatibility with the generic, but otherwise ignored.
#' @export
#' @examples
#' \dontrun{
#' # To connect to a database
#' my_db <- src_presto(catalog = "hive", schema = "web", user = "onur",
#'   host = "localhost", port = 8888, session.timezone='Asia/Kathmandu')
#' }
src_presto <- function(
    catalog=NULL,
    schema=NULL,
    user=NULL,
    host= NULL,
    port=NULL,
    session.timezone=NULL,
    parameters=NULL,
    ...
  ) {
  if(!requireNamespace('dplyr', quietly=TRUE)) {
    stop('This function requires the dplyr package, please install it first ',
         'and try again.')
  }

  con <- DBI::dbConnect(
    drv=Presto(),
    catalog=catalog %||% character(0),
    schema=schema %||% character(0),
    user=user %||% character(0),
    host=host %||% character(0),
    port=port %||% character(0),
    session.timezone=session.timezone %||% character(0),
    parameters=parameters %||% list(),
    ...
  )

  info <- DBI::dbGetInfo(con)

  return(dplyr::src_sql(
    "presto",
    con,
    info=info,
    disco=.db.disconnector(con)
  ))
}

.db.disconnector <- function(con) {
  reg.finalizer(environment(), function(...) {
    return(DBI::dbDisconnect(con))
  })
  environment()
}

"%||%" <- function(x, y) if (is.null(x)) return(y) else return(x)
