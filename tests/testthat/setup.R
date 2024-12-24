# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

presto_type <- Sys.getenv("PRESTO_TYPE", "Presto")
is_trino <- (presto_type == "Trino")
has_conn <- FALSE

# If the Presto/Trino server is started and the memory catalog is set up,
# populate an "iris" table in both "default" and "testing" schemas
tryCatch(
  {
    conn <- DBI::dbConnect(
      RPresto::Presto(),
      catalog = "memory",
      schema = "default",
      host = "http://localhost",
      port = ifelse(is_trino, 8090, 8080),
      source = "RPresto_test",
      user = Sys.getenv("USER"),
      use.trino.headers = is_trino
    )
    has_conn <- DBI::dbGetQuery(conn, "SELECT TRUE AS res")$res
  },
  error = function(...) {
    warning(
      presto_type, " connection default is not available",
      call. = FALSE, immediate. = TRUE
    )
  }
)

if (has_conn) {
  if (DBI::dbExistsTable(conn, "iris")) {
    stop(
      "iris table already exists in ", presto_type, " connection default",
      call. = FALSE
    )
  }
  DBI::dbWriteTable(conn, "iris", iris)
  if (DBI::dbExistsTable(conn, "iris")) {
    message(
      "iris table is successfully written in ",
      presto_type,
      " connection default"
    )
  } else {
    stop(
      "iris table failed to be written in ", presto_type, " connection default",
      call. = FALSE
    )
  }

  schemas <- DBI::dbGetQuery(conn, "SHOW SCHEMAS")$Schema
  has_testing_schema <- "testing" %in% schemas
  if (!has_testing_schema) {
    DBI::dbExecute(conn, "CREATE SCHEMA testing")
  }
  if (DBI::dbExistsTable(conn, dbplyr::in_schema("testing", "iris"))) {
    stop(
      "iris table already exists in ", presto_type, " connection testing",
      call. = FALSE
    )
  }
  DBI::dbWriteTable(conn, dbplyr::in_schema("testing", "iris"), iris)
  if (DBI::dbExistsTable(conn, dbplyr::in_schema("testing", "iris"))) {
    message(
      "iris table is successfully written in ",
      presto_type,
      " connection testing"
    )
  } else {
    stop(
      "iris table failed to be written in ", presto_type, " connection testing",
      call. = FALSE
    )
  }
}

withr::defer(
  {
    if (has_conn && DBI::dbExistsTable(conn, "iris")) {
      DBI::dbRemoveTable(conn, "iris")
    }
    if (has_conn && DBI::dbExistsTable(conn, dbplyr::in_schema("testing", "iris"))) {
      DBI::dbRemoveTable(conn, dbplyr::in_schema("testing", "iris"))
    }
    if (has_conn && !has_testing_schema) {
      DBI::dbExecute(conn, "DROP SCHEMA IF EXISTS testing")
    }
  },
  teardown_env()
)
