# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbExistsTable and db_has_table")

source("utilities.R")

test_that("dbExistsTable works with live database", {
  conn <- setup_live_connection()
  expect_false(dbExistsTable(conn, "_non_existent_table_"))
  expect_false(db_has_table(conn, "_non_existent_table_"))
  expect_true(dbExistsTable(conn, "iris"))
  expect_true(db_has_table(conn, "iris"))
  expect_true(dbExistsTable(conn, dbQuoteIdentifier(conn, "iris")))
  expect_true(db_has_table(conn, dbQuoteIdentifier(conn, "iris")))
})
