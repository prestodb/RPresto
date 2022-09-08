# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('dbWriteTable')

source('utilities.R')

test_that('dbWriteTable works with live connection', {
  conn <- setup_live_connection()
  test_table_name <- 'test_dbwritetable'
  test_df <- tibble::tibble(
    field1 = c('a', 'b'),
    field2 = c(1L, 2L),
    field3 = c(3.14, 2.72),
    field4 = c(TRUE, FALSE)
  )
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_true(dbWriteTable(conn, test_table_name, test_df))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
  expect_error(
    dbWriteTable(conn, test_table_name, test_df),
    'exists but overwrite is set to FALSE'
  )
  expect_message(
    res <- dbWriteTable(conn, test_table_name, test_df, overwrite = TRUE),
    'is overwritten'
  )
  expect_true(res)
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
})