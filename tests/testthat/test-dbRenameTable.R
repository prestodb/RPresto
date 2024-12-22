# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbRenameTable"))

.test_rename_table <- function(conn, test_table_name, test_new_table_name) {
  # create test table from origin table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  if (dbExistsTable(conn, test_new_table_name)) {
    dbRemoveTable(conn, test_new_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))
  expect_false(dbExistsTable(conn, test_new_table_name))
  expect_true(dbCreateTableAs(conn, test_table_name, "SELECT * FROM iris"))
  expect_true(dbExistsTable(conn, test_table_name))

  # test table rename
  expect_equal(dbRenameTable(conn, test_table_name, test_new_table_name), 0L)
  expect_false(dbExistsTable(conn, test_table_name))
  expect_true(dbExistsTable(conn, test_new_table_name))
  expect_true(dbRemoveTable(conn, test_new_table_name))
}

test_that("dbRenameTable works with live connection", {
  conn <- setup_live_connection()

  # table names
  test_table_name <- "test_renametable"
  test_new_table_name <- paste0(test_table_name, "_new")

  # character names work
  .test_rename_table(conn, test_table_name, test_new_table_name)
  # in_schema works
  .test_rename_table(
    conn,
    dbplyr::in_schema(conn@schema, test_table_name),
    dbplyr::in_schema(conn@schema, test_new_table_name)
  )
  # Id works
  .test_rename_table(
    conn,
    DBI::Id(table = test_table_name),
    DBI::Id(table = test_new_table_name)
  )
  # dbQuoteIdentifier works
  .test_rename_table(
    conn,
    DBI::dbQuoteIdentifier(conn, test_table_name),
    DBI::dbQuoteIdentifier(conn, test_new_table_name)
  )
})
