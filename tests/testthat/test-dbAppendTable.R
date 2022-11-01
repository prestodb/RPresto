# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbAppendTable")

source("utilities.R")

test_that("dbAppendTable works with live connection", {
  conn <- presto_default(output.timezone = "America/Los_Angeles")
  test_table_name <- "test_dbappendtable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_true(dbCreateTable(conn, test_table_name, test_df))
  expect_equal(
    dbAppendTable(conn, test_table_name, test_df),
    3L
  )
  expect_equal_data_frame(
    dbReadTable(conn, test_table_name),
    test_df
  )
})
