# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbCreateTable and sqlCreateTable"))

test_that("sqlCreateTable works", {
  conn <- setup_live_connection()
  test_table_name <- "test_sqlcreatetable"
  expect_equal(
    sqlCreateTable(
      conn, test_table_name,
      fields = c("field1" = "BIGINT", "field2" = "VARCHAR")
    ),
    DBI::SQL(
      paste(
        "CREATE TABLE", dbQuoteIdentifier(conn, test_table_name),
        '(\n  "field1" BIGINT,\n  "field2" VARCHAR\n)\n'
      )
    )
  )
  expect_equal(
    sqlCreateTable(
      conn, test_table_name,
      fields = c("field1" = "BIGINT", "field2" = "VARCHAR"),
      with = "WITH (format = 'ORC')"
    ),
    DBI::SQL(
      paste0(
        "CREATE TABLE ", dbQuoteIdentifier(conn, test_table_name),
        ' (\n  "field1" BIGINT,\n  "field2" VARCHAR\n)\n',
        "WITH (format = 'ORC')"
      )
    )
  )
})

test_that("dbCreateTable works with live database", {
  conn <- setup_live_connection()
  test_table_name <- "test_createtable"
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))
  expect_error(
    dbCreateTable(
      conn, test_table_name,
      fields = c("field1" = "BIGINT", "field2" = "VARCHAR"),
      temporary = TRUE
    ),
    "CREATE TEMPORARY TABLE is not supported in Presto"
  )
  expect_true(
    dbCreateTable(
      conn, test_table_name,
      fields = c("field1" = "BIGINT", "field2" = "VARCHAR")
    )
  )
  expect_true(dbExistsTable(conn, test_table_name))
  df.test_table <- dbGetQuery(
    conn, paste("SELECT * FROM", test_table_name, "LIMIT 0")
  )
  expect_equal(colnames(df.test_table), c("field1", "field2"))
  expect_equal(
    unname(vapply(df.test_table, typeof, character(1))),
    c("integer", "character")
  )
  expect_error(
    dbCreateTable(
      conn, test_table_name,
      fields = c("field1" = "BIGINT", "field2" = "VARCHAR")
    ),
    "Table .* already exists"
  )
})
