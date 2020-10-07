# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoDriver.R PrestoConnection.R PrestoResult.R
NULL

#' Metadata about database objects
#' @rdname dbGetInfo
#' @export
setMethod("dbGetInfo",
  "PrestoDriver",
  function(dbObj) {
    return()
  }
)

#' @rdname dbGetInfo
#' @export
setMethod("dbGetInfo",
  "PrestoConnection",
  function(dbObj) {
    return(list(
      host=dbObj@host,
      port=dbObj@port,
      user=dbObj@user,
      catalog=dbObj@catalog,
      schema=dbObj@schema
    ))
  }
)

.dbGetInfo.PrestoResult <- function(dbObj) {
  return(list(
    query.id=dbObj@query.id,
    statement=.dbGetStatement(dbObj),
    row.count=.dbGetRowCount(dbObj),
    has.completed=.dbHasCompleted(dbObj),
    stats=dbObj@cursor$stats()
  ))
}


#' For the \code{\linkS4class{PrestoResult}} object, the implementation
#' returns the additional \code{stats} field which can be used to
#' implement things like progress bars. See the examples section.
#' @param dbObj A \code{\linkS4class{PrestoDriver}},
#'          \code{\linkS4class{PrestoConnection}}
#'          or \code{\linkS4class{PrestoResult}} object
#' @return [PrestoResult] A \code{\link{list}} with elements
#'   \describe{
#'     \item{statement}{The SQL sent to the database}
#'     \item{row.count}{Number of rows fetched so far}
#'     \item{has.completed}{Whether all data has been fetched}
#'     \item{stats}{Current stats on the query}
#'   }
#' @rdname dbGetInfo
#' @export
#' @examples
#' \dontrun{
#'   conn <- dbConnect(Presto(), 'localhost', 7777, 'onur', 'datascience')
#'   result <- dbSendQuery(conn, 'SELECT * FROM jonchang_iris')
#'   iris <- data.frame()
#'   progress.bar <- NULL
#'   while (!dbHasCompleted(result)) {
#'     chunk <- dbFetch(result)
#'     if (!NROW(iris)) {
#'       iris <- chunk
#'     } else if (NROW(chunk)) {
#'       iris <- rbind(iris, chunk)
#'     }
#'     stats <- dbGetInfo(result)[['stats']]
#'     if (is.null(progress.bar)) {
#'       progress.bar <- txtProgressBar(0, stats[['totalSplits']], style=3)
#'     } else {
#'       setTxtProgressBar(progress.bar, stats[['completedSplits']])
#'     }
#'   }
#'   close(progress.bar)
#' }
setMethod("dbGetInfo", "PrestoResult", .dbGetInfo.PrestoResult)
