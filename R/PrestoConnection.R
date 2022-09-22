# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoSession.R
NULL

# Register request as a valid S3 class so that it can be used to specify the
# request.config slot in PrestoConnection below.
setOldClass("request")

#' S4 implementation of `DBIConnection` for Presto.
#'
#' @keywords internal
#' @importClassesFrom DBI DBIConnection
#' @export
setClass("PrestoConnection",
  contains = "DBIConnection",
  slots = c(
    "catalog" = "character",
    "schema" = "character",
    "user" = "character",
    "host" = "character",
    "port" = "integer",
    "source" = "character",
    "session.timezone" = "character",
    "output.timezone" = "character",
    "use.trino.headers" = "logical",
    "Id" = "character",
    "session" = "PrestoSession",
    "request.config" = "request",
    "extra.credentials" = "character",
    "bigint" = "character"
  )
)

#' @rdname PrestoConnection-class
#' @importMethodsFrom methods show
#' @export
setMethod(
  "show",
  "PrestoConnection",
  function(object) {
    cat(
      "<PrestoConnection: ", object@host, ":", object@port, ">\n",
      "Catalog: ", object@catalog, "\n",
      "Schema: ", object@schema, "\n",
      "User: ", object@user, "\n",
      "Source: ", object@source, "\n",
      "Session Time Zone: ", object@session.timezone, "\n",
      "Output Time Zone: ", object@output.timezone, "\n",
      "Extra Credentials: ", object@extra.credentials, "\n",
      "BIGINT cast to: ", object@bigint, "\n",
      sep = ""
    )
    parameters <- object@session$parameters()
    if (!is.null(parameters) && length(parameters)) {
      cat(
        "Parameters:\n",
        paste(
          "\t", names(parameters), ": ", unlist(parameters), "\n",
          sep = "",
          collapse = ""
        ),
        sep = ""
      )
    }
  }
)
