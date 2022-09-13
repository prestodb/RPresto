# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbRemoveTable")

source("utilities.R")

test_that("dbRemoveTable works with live database", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbremovetable"
  if (!dbExistsTable(conn, test_table_name)) {
    dbExecute(
      conn,
      paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
    )
  }
  expect_true(dbExistsTable(conn, test_table_name))
  expect_true(dbRemoveTable(conn, test_table_name))
  expect_false(dbExistsTable(conn, test_table_name))
})
