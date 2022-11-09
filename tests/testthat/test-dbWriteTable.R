# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbWriteTable and db_write_table")

source("utilities.R")

test_that("dbWriteTable works with live connection", {
  conn <- presto_default(output.timezone = "America/Los_Angeles")
  test_table_name <- "test_dbwritetable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_true(dbWriteTable(conn, test_table_name, test_df))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
  expect_true(dbRemoveTable(conn, test_table_name))
  expect_true(
    dbWriteTable(conn, test_table_name, test_df, use.one.query = TRUE)
  )
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
  expect_true(dbRemoveTable(conn, test_table_name))
  expect_true(dbWriteTable(conn, test_table_name, test_df, row.names = TRUE))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(
    dbListFields(conn, test_table_name),
    c("row_names", colnames(test_df))
  )
  expect_true(dbRemoveTable(conn, test_table_name))
  expect_true(
    dbWriteTable(conn, test_table_name, test_df, chunk.fields = c("field1"))
  )
  expect_equal_data_frame(
    dplyr::arrange(dbReadTable(conn, test_table_name), field1),
    dplyr::arrange(test_df, field1)
  )
  expect_error(
    dbWriteTable(conn, test_table_name, test_df),
    "exists in database, and both overwrite and append are FALSE"
  )
  expect_true(dbWriteTable(conn, test_table_name, test_df, overwrite = TRUE))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
  expect_error(
    dbWriteTable(
      conn, test_table_name, test_df, append = TRUE, overwrite = TRUE
    ),
    "overwrite and append cannot both be TRUE"
  )
  expect_true(dbWriteTable(conn, test_table_name, test_df, append = TRUE))
  expect_equal(
    nrow(dbReadTable(conn, test_table_name)),
    nrow(test_df) * 2L
  )
})

test_that("db_write_table works with live connection", {
  conn <- presto_default(output.timezone = "America/Los_Angeles")
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
    "exists in database, and both overwrite and append are FALSE"
  )
  expect_equal(
    res <- db_write_table(
      con = conn,
      table = test_table_name,
      types = NULL,
      values = test_df,
      overwrite = TRUE
    ),
    test_table_name
  )
  expect_true(db_has_table(conn, test_table_name))
  expect_equal_data_frame(dbReadTable(conn, test_table_name), test_df)
})
