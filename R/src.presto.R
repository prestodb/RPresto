# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' dplyr integration to connect to a Presto database.
#'
#' Allows you to connect to an existing database through a presto connection.
#'
#' @param catalog Catalog to use in the connection
#' @param schema Schema to use in the connection
#' @param user User name to use in the connection
#' @param host Host name to connect to the database
#' @param port Port number to use with the host name
#' @param source Source to specify for the connection
#' @param session.timezone Time zone for the connection
#' @param parameters Additional parameters to pass to the connection
#' @param bigint The R type that Presto's 64-bit integer (`BIGINT`) types should
#'          be translated to. The default is `"integer"`, which returns R's
#'          `integer` type, but results in `NA` for values above/below
#'          +/-2147483647. `"integer64"` returns a [bit64::integer64], which
#'          allows the full range of 64 bit integers. `"numeric"` coerces into
#'          R's `double` type but might result in precision loss. Lastly,
#'          `"character"` casts into R's `character` type.
#' @param ... For \code{src_presto} other arguments passed on to the underlying
#'   database connector \code{dbConnect}. For \code{tbl.src_presto}, it is
#'   included for compatibility with the generic, but otherwise ignored.
#' @export
#' @name src_presto
#' @examples
#' \dontrun{
#' # To connect to a database
#' my_db <- src_presto(
#'   catalog = "memory",
#'   schema = "default",
#'   user = Sys.getenv("USER"),
#'   host = "http://localhost",
#'   port = 8080,
#'   session.timezone = "Asia/Kathmandu"
#' )
#' }
src_presto <- function(
    catalog=NULL,
    schema=NULL,
    user=NULL,
    host= NULL,
    port=NULL,
    source=NULL,
    session.timezone=NULL,
    parameters=NULL,
    bigint = c("integer", "integer64", "numeric", "character"),
    ...
  ) {
  if(!requireNamespace('dplyr', quietly=TRUE)) {
    stop('src_presto requires the dplyr package, please install it first ',
         'and try again.')
  }

  con <- DBI::dbConnect(
    drv=Presto(),
    catalog=catalog %||% character(0),
    schema=schema %||% character(0),
    user=user %||% character(0),
    host=host %||% character(0),
    port=port %||% character(0),
    source=source %||% character(0),
    session.timezone=session.timezone %||% character(0),
    parameters=parameters %||% list(),
    bigint=match.arg(bigint) %||% character(0),
    ...
  )

  if (utils::packageVersion('dplyr') >= '0.5.0.9004') {
    if (!requireNamespace('dbplyr', quietly=TRUE)) {
      stop('src_presto requires the dbplyr package, please install it first ',
           'and try again'
      )
    }
    src_function <- utils::getFromNamespace('src_dbi', 'dbplyr')
    src <- src_function(con, auto_disconnect=FALSE)
  } else {
    info <- DBI::dbGetInfo(con)
    src_function <- utils::getFromNamespace('src_sql', 'dplyr')
    src <- src_function(
      "presto",
      con,
      info=info,
      disco=.db.disconnector(con)
    )
  }
  class(src) <- union('src_presto', class(src))
  return(src)
}

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

.db.disconnector <- function(con) {
  reg.finalizer(environment(), function(...) {
    return(DBI::dbDisconnect(con))
  })
  environment()
}

"%||%" <- function(x, y) if (is.null(x)) return(y) else return(x)
