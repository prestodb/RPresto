# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoQuery.R PrestoConnection.R
NULL

#' An S4 class to represent a Presto Result
#' @slot statement The SQL statement sent to the database
#' @slot connection The connection object associated with the result
#' @slot query An internal implementation detail for keeping track of
#'  what stage a request is in
#' @slot post.data Any data extracted from the POST request response
#' @slot bigint How bigint type should be handled
#' @keywords internal
#' @importClassesFrom DBI DBIResult
#' @export
setClass("PrestoResult",
  contains = "DBIResult",
  slots = c(
    "statement" = "character",
    "connection" = "PrestoConnection",
    "query" = "PrestoQuery",
    "post.data" = "ANY",
    "bigint" = "character"
  )
)

#' @rdname PrestoResult-class
#' @importMethodsFrom methods show
#' @export
setMethod(
  "show",
  "PrestoResult",
  function(object) {
    query_stats <- object@query$stats()

    cat(
      "<PrestoResult: ", object@query$id(), ">\n",
      "Status Code: ", httr::status_code(object@query$response()), "\n",
      "State: ", object@query$state(), "\n",
      "Info URI: ", object@query$infoUri(), "\n",
      "Next URI: ", object@query$nextUri(), "\n",
      "Splits (Queued/Running/Completed/Total): ",
      paste(
        c(
          query_stats[["queuedSplits"]],
          query_stats[["runningSplits"]],
          query_stats[["completedSplits"]],
          query_stats[["totalSplits"]]
        ),
        collapse = " / "
      ), "\n",
      "Session Time Zone: ", object@connection@session.timezone, "\n",
      "Output Time Zone: ", object@connection@output.timezone, "\n",
      "BIGINT cast to: ", object@connection@bigint, "\n",
      sep = ""
    )
  }
)
