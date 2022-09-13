# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbWriteTable and db_write_table")

source("utilities.R")

test_df <- tibble::tibble(
  field1 = c("a", "b"),
  field2 = c(1L, 2L),
  field3 = c(3.14, 2.72),
  field4 = c(TRUE, FALSE)
)

test_that("dbWriteTable works with live connection", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbwritetable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_true(dbWriteTable(conn, test_table_name, test_df))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
  expect_error(
    dbWriteTable(conn, test_table_name, test_df),
    "exists but overwrite is set to FALSE"
  )
  expect_message(
    res <- dbWriteTable(conn, test_table_name, test_df, overwrite = TRUE),
    "is overwritten"
  )
  expect_true(res)
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
})

test_that("db_write_table works with live connection", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbwritetable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_equal(
    db_write_table(
      con = conn,
      table = test_table_name,
      types = NULL,
      values = test_df
    ),
    test_table_name
  )
  expect_true(db_has_table(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
  expect_error(
    db_write_table(
      con = conn,
      table = test_table_name,
      types = NULL,
      values = test_df
    ),
    "exists but overwrite is set to FALSE"
  )
  expect_message(
    res <- db_write_table(
      con = conn,
      table = test_table_name,
      types = NULL,
      values = test_df,
      overwrite = TRUE
    ),
    "is overwritten"
  )
  expect_equal(res, test_table_name)
  expect_true(db_has_table(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
})
