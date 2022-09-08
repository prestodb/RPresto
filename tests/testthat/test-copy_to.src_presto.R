# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('copy_to.src_presto')

source('utilities.R')

.test_src <- function(src, test_table_name) {
  if (inherits(src, 'src_presto')) {
    con <- src$con
  }
  if (inherits(src, 'PrestoConnection')) {
    con <- src
  }
  test_df <- tibble::tibble(
    field1 = c('a', 'b'),
    field2 = c(1L, 2L),
    field3 = c(3.14, 2.72),
    field4 = c(TRUE, FALSE)
  )
  if (dbExistsTable(con, test_table_name)) {
    dbRemoveTable(con, test_table_name)
  }
  tbl <- copy_to(dest = src, df = test_df, name = test_table_name)
  expect_true(dbExistsTable(con, test_table_name))
  expect_equal_data_frame(collect(tbl), test_df)
  expect_error(
    tbl <- copy_to(dest = src, df = test_df, name = test_table_name),
    'exists but overwrite is set to FALSE'
  )
  expect_message(
    tbl <- copy_to(
      dest = src, df = test_df, name = test_table_name, overwrite = TRUE
    ),
    'is overwritten'
  )
  expect_true(dbExistsTable(con, test_table_name))
  expect_equal_data_frame(collect(tbl), test_df)
}

test_that('dplyr::copy_to works for src_presto', {
  src <- setup_live_dplyr_connection()[['db']]
  test_table_name <- "test_copyto_srcpresto"
  .test_src(src, test_table_name)
})

test_that('dplyr::copy_to works for PrestoConnection', {
  src <- setup_live_connection()
  test_table_name <- "test_copyto_prestoconnection"
  .test_src(src, test_table_name)
})
