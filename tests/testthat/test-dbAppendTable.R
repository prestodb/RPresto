# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbAppendTable"))

test_that("dbAppendTable works with live connection", {
  conn <- setup_live_connection(output.timezone = "America/Los_Angeles")
  test_table_name <- "test_dbappendtable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_true(dbCreateTable(conn, test_table_name, test_df))
  # character name works
  expect_equal(
    dbAppendTable(conn, test_table_name, test_df),
    3L
  )
  expect_equal_data_frame(
    dbReadTable(conn, test_table_name),
    test_df
  )
  # in_schema() works
  expect_equal(
    dbAppendTable(conn, dbplyr::in_schema(conn@schema, test_table_name), test_df),
    3L
  )
  # Id() works
  expect_equal(
    dbAppendTable(conn, DBI::Id(table = test_table_name), test_df),
    3L
  )
  # dbQuoteIdentifier() works
  expect_equal(
    dbAppendTable(conn, DBI::dbQuoteIdentifier(conn, test_table_name), test_df),
    3L
  )
})
