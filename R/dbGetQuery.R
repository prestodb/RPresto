# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoConnection.R
NULL

.dbGetQuery <- function(
  conn, statement, ...,
  statement_with_cte = NULL,
  cte_tables = c(),
  quiet = getOption("rpresto.quiet")
) {
  result <- dbSendQuery(
    conn, statement_with_cte %||% statement, quiet = quiet, ...
  )
  on.exit(dbClearResult(result))
  res <- try(
    {
      dbFetch(result, -1)
    },
    silent = TRUE
  )
  # If error, try CTE; return if not
  if (inherits(res, "try-error")) {
    matching_cte_table <- find_cte_tables_from_try_result(conn, res)
    # If found CTE, retry; return error if not
    if (length(matching_cte_table) > 0) {
      # Add previously found CTE tables for the retry. The order of the CTE
      # tables matter.
      cte_tables <- unique(c(matching_cte_table, cte_tables))
      sql_with_cte <- generate_sql_with_cte(conn, statement, cte_tables)
      # Retry using the same function
      res <- .dbGetQuery(
        conn = conn,
        statement = statement,
        ...,
        statement_with_cte = sql_with_cte,
        cte_tables = cte_tables
      )
      return(res)
    }
    stop(conditionMessage(attr(res, "condition")), call. = FALSE)
  }
  return(res)
}

#' @rdname PrestoConnection-class
#' @importMethodsFrom DBI dbGetQuery
#' @export
setMethod(
  "dbGetQuery",
  c("PrestoConnection", "character"),
  function(conn, statement, ..., quiet = getOption("rpresto.quiet")) {
    .dbGetQuery(conn, statement, quiet = quiet, ...)
  }
)
