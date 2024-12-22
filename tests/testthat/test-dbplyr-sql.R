# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbplyr-sql"))

test_that("save_query_save works", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbquerysave"
  sql <- "SELECT * FROM iris"
  expect_error(
    sql_query_save(conn, sql, test_table_name),
    "Temporary table is not supported"
  )
  expect_equal(
    sql_query_save(conn, sql, test_table_name, temporary = FALSE),
    DBI::SQL(
      paste0(
        "CREATE TABLE ", dbQuoteIdentifier(conn, test_table_name), "\n",
        "AS\n",
        sql
      )
    )
  )
  with_statement <- "WITH (format = 'ORC')"
  expect_equal(
    sql_query_save(
      conn, sql, test_table_name,
      temporary = FALSE,
      with = with_statement
    ),
    DBI::SQL(
      paste0(
        "CREATE TABLE ", dbQuoteIdentifier(conn, test_table_name), "\n",
        with_statement, "\n",
        "AS\n",
        sql
      )
    )
  )
})
