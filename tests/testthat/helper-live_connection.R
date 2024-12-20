# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

read_credentials <- function() {
  if (!file.exists("credentials.dcf")) {
    skip(paste0(
      "credential file missing, please create a file ",
      system.file("tests", "testthat", "credentials.dcf", package = "RPresto"),
      ' with fields "host", "port", "source", "catalog", "schema" to do live testing'
    ))
  }
  dcf <- read.dcf("credentials.dcf")
  credentials <- list(
    host = as.vector(dcf[1, "host"]),
    port = as.integer(as.vector(dcf[1, "port"])),
    catalog = as.vector(dcf[1, "catalog"]),
    schema = as.vector(dcf[1, "schema"]),
    iris_table_name = as.vector(dcf[1, "iris_table_name"]),
    source = as.vector(dcf[1, "source"])
  )
  return(credentials)
}

setup_live_connection <- function(session.timezone = "",
                                  parameters = list(),
                                  extra.credentials = "",
                                  bigint = c("integer", "integer64", "numeric", "character"),
                                  ...,
                                  type = "Presto") {
  skip_on_cran()
  if (type == "Presto") {
    credentials <- read_credentials()
    conn <- dbConnect(RPresto::Presto(),
                      schema = credentials$schema,
                      catalog = credentials$catalog,
                      host = credentials$host,
                      port = credentials$port,
                      source = credentials$source,
                      session.timezone = session.timezone,
                      parameters = parameters,
                      extra.credentials = extra.credentials,
                      user = Sys.getenv("USER"),
                      bigint = bigint,
                      ...
    )
  } else if (type == "Trino") {
    conn <- dbConnect(RPresto::Presto(),
                      use.trino.headers = TRUE,
                      schema = "default",
                      catalog = "memory",
                      host = "http://localhost",
                      port = 8090,
                      source = "RPresto_test",
                      session.timezone = session.timezone,
                      parameters = parameters,
                      extra.credentials = extra.credentials,
                      user = Sys.getenv("USER"),
                      bigint = bigint,
                      ...
    )
  } else {
    stop("Connection type is not Presto or Trino.", call. = FALSE)
  }
  return(conn)
}

setup_live_dplyr_connection <- function(session.timezone = "",
                                        parameters = list(),
                                        extra.credentials = "",
                                        bigint = c("integer", "integer64", "numeric", "character"),
                                        ...,
                                        type = "Presto") {
  skip_on_cran()

  credentials <- read_credentials()
  db <- src_presto(
    con = setup_live_connection(
      session.timezone,
      parameters,
      extra.credentials,
      bigint,
      ...,
      type = type
    )
  )
  return(list(db = db, iris_table_name = credentials[["iris_table_name"]]))
}
