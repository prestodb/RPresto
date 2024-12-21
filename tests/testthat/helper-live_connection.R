# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

setup_live_connection <- function(schema = "default",
                                  session.timezone = "",
                                  parameters = list(),
                                  extra.credentials = "",
                                  bigint = c("integer", "integer64", "numeric", "character"),
                                  ...,
                                  type = "Presto") {
  skip_on_cran()
  if (type == "Presto") {
    conn <- dbConnect(RPresto::Presto(),
                      schema = schema,
                      catalog = "memory",
                      host = "http://localhost",
                      port = 8080,
                      source = "RPresto_test",
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
                      schema = schema,
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

setup_live_dplyr_connection <- function(schema = "default",
                                        session.timezone = "",
                                        parameters = list(),
                                        extra.credentials = "",
                                        bigint = c("integer", "integer64", "numeric", "character"),
                                        ...,
                                        type = "Presto") {
  skip_on_cran()

  db <- src_presto(
    con = setup_live_connection(
      schema = schema,
      session.timezone = session.timezone,
      parameters = parameters,
      extra.credentials = extra.credentials,
      bigint = bigint,
      ...,
      type = type
    )
  )
  return(list(db = db, iris_table_name = "iris"))
}
