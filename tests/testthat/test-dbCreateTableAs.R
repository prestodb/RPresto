# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbCreateTableAs and sqlCreateTableAs"))

test_that("sqlCreateTableAs works", {
  conn <- setup_live_connection()
  test_table_name <- "test_sqlcreatetableas"
  expect_equal(
    sqlCreateTableAs(
      conn, test_table_name,
      sql = "SELECT * FROM iris"
    ),
    DBI::SQL(
      paste0(
        "CREATE TABLE ", dbQuoteIdentifier(conn, test_table_name), "\n",
        "AS\n",
        "SELECT * FROM iris"
      )
    )
  )
  expect_equal(
    sqlCreateTableAs(
      conn, test_table_name,
      sql = "SELECT * FROM iris",
      with = "WITH (format = 'ORC')"
    ),
    DBI::SQL(
      paste0(
        "CREATE TABLE ", dbQuoteIdentifier(conn, test_table_name), "\n",
        "WITH (format = 'ORC')\n",
        "AS\n",
        "SELECT * FROM iris"
      )
    )
  )
})

test_equal_tables <- function(conn, test_table_name, test_origin_table) {
  expect_equal(
    dbListFields(conn, test_table_name),
    dbListFields(conn, test_origin_table)
  )
  expect_equal(
    get_nrow(conn, test_table_name),
    get_nrow(conn, test_origin_table)
  )
}

test_that("dbCreateTableAS works with live database", {
  conn <- setup_live_connection()
  test_table_name <- "test_createtableas"
  test_origin_table <- "iris"
  test_statement <- paste0("SELECT * FROM ", test_origin_table)
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))
  expect_true(dbCreateTableAs(conn, test_table_name, test_statement))
  expect_true(dbExistsTable(conn, test_table_name))
  test_equal_tables(conn, test_table_name, test_origin_table)
  expect_error(
    dbCreateTableAs(conn, test_table_name, test_statement),
    "The table .* exists but overwrite is set to FALSE"
  )
  expect_message(
    res <- dbCreateTableAs(
      conn, test_table_name, test_statement,
      overwrite = TRUE
    ),
    "is overwritten"
  )
  expect_true(res)
  expect_true(dbExistsTable(conn, test_table_name))
  test_equal_tables(conn, test_table_name, test_origin_table)
})
