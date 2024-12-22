# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbRemoveTable"))

test_that("dbRemoveTable works with live database", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbremovetable"
  if (!dbExistsTable(conn, test_table_name)) {
    DBI::dbExecute(
      conn,
      paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
    )
  }
  expect_true(dbExistsTable(conn, test_table_name))
  # character name works
  expect_true(dbRemoveTable(conn, test_table_name))
  expect_false(dbExistsTable(conn, test_table_name))
  # in_schema() works
  DBI::dbExecute(
    conn,
    paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
  )
  expect_true(dbExistsTable(conn, test_table_name))
  expect_true(dbRemoveTable(conn, dbplyr::in_schema(conn@schema, test_table_name)))
  expect_false(dbExistsTable(conn, test_table_name))
  # Id() works
  DBI::dbExecute(
    conn,
    paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
  )
  expect_true(dbExistsTable(conn, test_table_name))
  expect_true(dbRemoveTable(conn, DBI::Id(table = test_table_name)))
  expect_false(dbExistsTable(conn, test_table_name))
  # dbQuoteIdentfier() works
  DBI::dbExecute(
    conn,
    paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
  )
  expect_true(dbExistsTable(conn, test_table_name))
  expect_true(dbRemoveTable(conn, DBI::dbQuoteIdentifier(conn, test_table_name)))
  expect_false(dbExistsTable(conn, test_table_name))
})
