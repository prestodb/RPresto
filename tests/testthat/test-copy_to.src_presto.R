# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "copy_to.src_presto and db_copy_to"))

.test_src <- function(src, test_table_name) {
  if (inherits(src, "src_presto")) {
    con <- src$con
  }
  if (inherits(src, "PrestoConnection")) {
    con <- src
  }
  if (dbExistsTable(con, test_table_name)) {
    dbRemoveTable(con, test_table_name)
  }
  # character name works
  tbl <- copy_to(dest = src, df = test_df, name = test_table_name)
  expect_true(dbExistsTable(con, test_table_name))
  expect_equal_data_frame(collect(tbl), test_df)
  expect_true(dbRemoveTable(con, test_table_name))
  # in_schema() name works
  test_table_name_2 <- dbplyr::in_schema(con@schema, test_table_name)
  tbl <- copy_to(dest = src, df = test_df, name = test_table_name_2)
  expect_true(dbExistsTable(con, test_table_name_2))
  expect_equal_data_frame(collect(tbl), test_df)
  expect_true(dbRemoveTable(con, test_table_name_2))
  # Id() name works
  test_table_name_3 <- DBI::Id(table = test_table_name)
  tbl <- copy_to(dest = src, df = test_df, name = test_table_name_3)
  expect_true(dbExistsTable(con, test_table_name_3))
  expect_equal_data_frame(collect(tbl), test_df)
  # overwrite warning
  expect_error(
    tbl <- copy_to(dest = src, df = test_df, name = test_table_name),
    "exists in database, and both overwrite and append are FALSE"
  )
  expect_message(
    tbl <- copy_to(
      dest = src, df = test_df, name = test_table_name, overwrite = TRUE
    ),
    "The table .* is overwritten"
  )
  expect_true(dbExistsTable(con, test_table_name))
  expect_equal_data_frame(collect(tbl), test_df)
  # a different schema works
  DBI::dbExecute(con, paste0("CREATE SCHEMA IF NOT EXISTS testing"))
  test_table_name_4 <- dbplyr::in_schema("testing", test_table_name)
  tbl <- copy_to(dest = src, df = test_df, name = test_table_name_4)
  expect_true(dbExistsTable(con, test_table_name_4))
  expect_equal_data_frame(collect(tbl), test_df)
  # overwriting a different schema works
  expect_message(
    tbl <- copy_to(
      dest = src, df = test_df, name = test_table_name_4, overwrite = TRUE
    ),
    "The table .* is overwritten"
  )
  expect_true(dbExistsTable(con, test_table_name_4))
  expect_equal_data_frame(collect(tbl), test_df)
  expect_true(dbRemoveTable(con, test_table_name_4))
}

test_that("dplyr::copy_to works for src_presto", {
  src <- src_presto(
    con = setup_live_connection(output.timezone = "America/Los_Angeles")
  )
  test_table_name <- "test_copyto_srcpresto"
  .test_src(src, test_table_name)
})

test_that("dplyr::copy_to works for PrestoConnection", {
  src <- setup_live_connection(output.timezone = "America/Los_Angeles")
  test_table_name <- "test_copyto_prestoconnection"
  .test_src(src, test_table_name)
})

test_that("dbplyr::db_copy_to works for PrestoConnection", {
  con <- setup_live_connection(output.timezone = "America/Los_Angeles")
  test_table_name <- "test_dbcopyto"
  if (dbExistsTable(con, test_table_name)) {
    dbRemoveTable(con, test_table_name)
  }
  expect_error(
    db_copy_to(
      con = con,
      table = test_table_name,
      values = test_df
    ),
    "Temporary tables not supported by RPresto"
  )
  expect_equal(
    db_copy_to(
      con = con,
      table = test_table_name,
      values = test_df,
      temporary = FALSE
    ),
    test_table_name
  )
  expect_true(db_has_table(con, test_table_name))
  expect_equal_data_frame(dbReadTable(con, test_table_name), test_df)
})
