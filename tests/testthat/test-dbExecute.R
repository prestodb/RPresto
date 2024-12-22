# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbExecute and dbGetRowsAffected"))

test_that("dbExecute works with live database to create empty table", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbexecute_empty_table"
  if (dbExistsTable(conn, test_table_name)) {
    # Removing existing test table should be accomplished by dbRemoveTable()
    # or dbExecute(). But since we're testing dbExecute() here, we use
    # dbGetQuery() to achieve the same effect.
    dbGetQuery(conn, paste0("DROP TABLE ", test_table_name))
  }
  expect_equal(
    DBI::dbExecute(
      conn,
      paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
    ),
    0
  )
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(
    dbListFields(conn, test_table_name),
    c("field1", "field2")
  )
})

test_that("dbExecute works with live database to replicate existing table", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbexecute_existing_table"
  if (dbExistsTable(conn, test_table_name)) {
    # Removing existing test table should be accomplished by dbRemoveTable()
    # or dbExecute(). But since we're testing dbExecute() here, we use
    # dbGetQuery() to achieve the same effect.
    dbGetQuery(conn, paste0("DROP TABLE ", test_table_name))
  }
  expect_equal(
    DBI::dbExecute(
      conn,
      paste("CREATE TABLE", test_table_name, "AS SELECT * FROM iris")
    ),
    NROW(iris_df)
  )
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(
    dbListFields(conn, test_table_name),
    dbListFields(conn, "iris"),
  )
})

test_that("dbExecute works with live database to insert values", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbexecute_insert_values"
  if (!dbExistsTable(conn, test_table_name)) {
    dbGetQuery(
      conn,
      paste("CREATE TABLE", test_table_name, "(field1 BIGINT, field2 VARCHAR)")
    )
  }
  old_nrow <- get_nrow(conn, test_table_name)
  expect_equal(
    DBI::dbExecute(
      conn,
      paste("INSERT INTO", test_table_name, "VALUES (1, 'a'), (2, 'b')")
    ),
    2
  )
  expect_equal(
    get_nrow(conn, test_table_name),
    old_nrow + 2
  )
})

test_that("dbExecute works with live database to rename table", {
  conn <- setup_live_connection()
  old_test_table_name <- "test_dbexecute_rename_table"
  new_test_table_name <- "test_dbexecute_rename_table2"
  if (!dbExistsTable(conn, old_test_table_name)) {
    dbGetQuery(
      conn,
      paste(
        "CREATE TABLE", old_test_table_name, "(field1 BIGINT, field2 VARCHAR)"
      )
    )
  }
  if (dbExistsTable(conn, new_test_table_name)) {
    dbGetQuery(
      conn,
      paste("DROP TABLE", new_test_table_name)
    )
  }
  expect_equal(
    DBI::dbExecute(
      conn,
      paste(
        "ALTER TABLE", old_test_table_name, "RENAME TO", new_test_table_name
      )
    ),
    0
  )
  expect_false(dbExistsTable(conn, old_test_table_name))
  expect_true(dbExistsTable(conn, new_test_table_name))
})
