# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbReadTable")

source("utilities.R")

test_that("dbReadTable works with live database", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbreadtable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  test_statement <- "SELECT CAST(1 AS BIGINT) AS field1"
  expect_true(dbCreateTableAs(conn, test_table_name, test_statement))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(
    dbReadTable(conn, test_table_name),
    tibble::tibble(field1 = 1L)
  )
  # in_schema() works
  expect_equal_data_frame(
    dbReadTable(conn, dbplyr::in_schema(conn@schema, test_table_name)),
    tibble::tibble(field1 = 1L)
  )
  # Id() works
  expect_equal_data_frame(
    dbReadTable(conn, DBI::Id(table = test_table_name)),
    tibble::tibble(field1 = 1L)
  )
  # dbQuoteIdentifier() works
  expect_equal_data_frame(
    dbReadTable(conn, DBI::dbQuoteIdentifier(conn, test_table_name)),
    tibble::tibble(field1 = 1L)
  )
  # bigint works
  expect_equal_data_frame(
    dbReadTable(conn, test_table_name, bigint = "character"),
    tibble::tibble(field1 = "1")
  )
  expect_true(dbRemoveTable(conn, test_table_name))
})

