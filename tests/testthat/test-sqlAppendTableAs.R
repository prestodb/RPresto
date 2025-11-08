# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "sqlAppendTableAs"))

test_that("sqlAppendTableAs works", {
  conn <- setup_live_connection()
  test_table_name <- "test_sqlappendtableas"
  test_origin_table <- "iris"
  
  # Create target table first (INSERT INTO requires existing table)
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  on.exit(dbRemoveTable(conn, test_table_name), add = TRUE)
  
  expect_equal(
    sqlAppendTableAs(
      conn, test_table_name,
      sql = "SELECT * FROM iris"
    ),
    DBI::SQL(
      paste0(
        "INSERT INTO ", dbQuoteIdentifier(conn, test_table_name), "\n",
        "SELECT * FROM iris"
      )
    )
  )
})

test_that("sqlAppendTableAs works with tbl_presto", {
  conn <- setup_live_connection()
  test_table_name <- "test_sqlappendtableas_tbl"
  test_origin_table <- "iris"
  
  # Create target table first (INSERT INTO requires existing table)
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  on.exit(dbRemoveTable(conn, test_table_name), add = TRUE)
  
  tbl_obj <- dplyr::tbl(conn, test_origin_table) %>%
    dplyr::filter(sepal.length > 5.0)
  
  result <- sqlAppendTableAs(conn, test_table_name, sql = tbl_obj)
  
  # Verify it's a SQL object
  expect_true(inherits(result, "SQL"))
  
  # Verify it contains INSERT INTO
  expect_true(grepl("INSERT INTO", as.character(result)))
  expect_true(grepl(test_table_name, as.character(result)))
})

test_that("sqlAppendTableAs works with different table name formats", {
  conn <- setup_live_connection()
  test_table_name <- "test_table"
  test_sql <- "SELECT * FROM iris"
  
  # Create target table first (INSERT INTO requires existing table)
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM iris WHERE 1 = 0")
  )
  on.exit(dbRemoveTable(conn, test_table_name), add = TRUE)
  
  # Character string
  result1 <- sqlAppendTableAs(conn, test_table_name, sql = test_sql)
  expect_true(inherits(result1, "SQL"))
  
  # Id()
  result2 <- sqlAppendTableAs(
    conn, DBI::Id(table = test_table_name), sql = test_sql
  )
  expect_true(inherits(result2, "SQL"))
  
  # SQL()
  result3 <- sqlAppendTableAs(
    conn, DBI::SQL(paste0('"', test_table_name, '"')), sql = test_sql
  )
  expect_true(inherits(result3, "SQL"))
})

test_that("sqlAppendTableAs validates sql input", {
  conn <- setup_live_connection()
  test_table_name <- "test_sqlappendtableas"
  
  # Create target table first (INSERT INTO requires existing table)
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    "SELECT 1 AS col1 WHERE 1 = 0"
  )
  on.exit(dbRemoveTable(conn, test_table_name), add = TRUE)
  
  # Valid character string
  result <- sqlAppendTableAs(conn, test_table_name, sql = "SELECT 1 AS col1")
  expect_true(inherits(result, "SQL"))
  
  # Invalid: multiple strings
  expect_error(
    sqlAppendTableAs(conn, test_table_name, sql = c("SELECT 1", "SELECT 2")),
    "length\\(sql\\) == 1"
  )
  
  # Invalid: not character or tbl_presto
  expect_error(
    sqlAppendTableAs(conn, test_table_name, sql = 123),
    "sql must be a character string or a tbl_presto object"
  )
})

test_that("sqlAppendTableAs errors when table doesn't exist", {
  conn <- setup_live_connection()
  test_table_name <- "test_sqlappendtableas_nonexistent"
  test_sql <- "SELECT * FROM iris LIMIT 1"
  
  # Ensure table doesn't exist
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  
  # Should error because table doesn't exist
  expect_error(
    sqlAppendTableAs(conn, test_table_name, sql = test_sql),
    "Table.*does not exist.*INSERT INTO requires an existing table"
  )
})

