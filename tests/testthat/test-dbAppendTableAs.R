# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbAppendTableAs"))

test_that("dbAppendTableAs works with simple SELECT query", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas"
  test_origin_table <- "iris"
  
  # Create target table with same structure
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  
  # Get initial row count
  initial_nrow <- get_nrow(conn, test_table_name)
  expect_equal(initial_nrow, 0L)
  
  # Append from SELECT query
  test_sql <- paste0(
    "SELECT * FROM ", test_origin_table, " WHERE \"sepal.length\" > 7.0"
  )
  rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql)
  
  # Verify rows were inserted
  expect_true(rows_inserted > 0L)
  expect_equal(get_nrow(conn, test_table_name), rows_inserted)
})

test_that("dbAppendTableAs works with tbl_presto", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_tbl"
  test_origin_table <- "iris"
  
  # Create target table with same structure
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  
  # Append from tbl_presto
  tbl_obj <- dplyr::tbl(conn, test_origin_table) %>%
    dplyr::filter(sepal.length > 7.0)
  
  rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = tbl_obj)
  
  # Verify rows were inserted
  expect_true(rows_inserted > 0L)
  expect_equal(get_nrow(conn, test_table_name), rows_inserted)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs works with complex queries", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_complex"
  test_origin_table <- "iris"
  
  # Create target table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0(
      "SELECT \"species\", AVG(\"sepal.length\") AS avg_length ",
      "FROM ", test_origin_table, " GROUP BY \"species\""
    )
  )
  
  # Append aggregated data
  test_sql <- paste0(
    "SELECT \"species\", AVG(\"sepal.width\") AS avg_length ",
    "FROM ", test_origin_table, " ",
    "WHERE \"sepal.length\" > 7.0 ",
    "GROUP BY \"species\""
  )
  initial_nrow <- get_nrow(conn, test_table_name)
  rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql)
  
  expect_true(rows_inserted > 0L)
  expect_equal(get_nrow(conn, test_table_name), initial_nrow + rows_inserted)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs works with empty result set", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_empty"
  test_origin_table <- "iris"
  
  # Create target table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  
  # Append from query that returns no rows
  test_sql <- paste0(
    "SELECT * FROM ", test_origin_table, " WHERE 1 = 0"
  )
  rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql)
  
  # Should return 0 and not error
  expect_equal(rows_inserted, 0L)
  expect_equal(get_nrow(conn, test_table_name), 0L)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs works with different table name formats", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_formats"
  test_origin_table <- "iris"
  
  # Create target table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  
  test_sql <- paste0(
    "SELECT * FROM ", test_origin_table, " LIMIT 1"
  )
  
  # Character string
  rows1 <- dbAppendTableAs(conn, test_table_name, sql = test_sql)
  expect_true(rows1 > 0L)
  
  # Id()
  rows2 <- dbAppendTableAs(
    conn, DBI::Id(table = test_table_name), sql = test_sql
  )
  expect_true(rows2 > 0L)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs errors when table doesn't exist", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_nonexistent"
  test_sql <- "SELECT * FROM iris LIMIT 1"
  
  # Ensure table doesn't exist
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  
  # Should error because table doesn't exist
  expect_error(
    dbAppendTableAs(conn, test_table_name, sql = test_sql),
    "Table.*does not exist.*INSERT INTO requires an existing table"
  )
})

test_that("dbAppendTableAs errors on invalid input", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas"
  
  # Invalid: not character or tbl_presto
  expect_error(
    dbAppendTableAs(conn, test_table_name, sql = 123),
    "sql must be a character string or a tbl_presto object"
  )
  
  # Invalid: multiple strings
  expect_error(
    dbAppendTableAs(conn, test_table_name, sql = c("SELECT 1", "SELECT 2")),
    "length\\(sql\\) == 1"
  )
})

test_that("dbAppendTableAs validates column names match", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_validate"
  test_origin_table <- "iris"
  
  # Create target table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT \"sepal.length\", \"sepal.width\" FROM ", test_origin_table, " WHERE 1 = 0")
  )
  
  # Test with different column names
  test_sql <- paste0(
    "SELECT \"petal.length\", \"petal.width\" FROM ", test_origin_table, " LIMIT 1"
  )
  
  expect_error(
    dbAppendTableAs(conn, test_table_name, sql = test_sql),
    "Column mismatch detected"
  )
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs reorders columns when auto_reorder is TRUE", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_reorder"
  test_origin_table <- "iris"
  
  # Create target table with specific column order
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0(
      "SELECT \"sepal.length\", \"sepal.width\", \"petal.length\", \"petal.width\" ",
      "FROM ", test_origin_table, " WHERE 1 = 0"
    )
  )
  
  # Query with different column order
  test_sql <- paste0(
    "SELECT \"petal.width\", \"sepal.length\", \"petal.length\", \"sepal.width\" ",
    "FROM ", test_origin_table, " LIMIT 1"
  )
  
  # Should reorder and show message
  expect_message(
    rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql, auto_reorder = TRUE),
    "Column order mismatch detected"
  )
  
  expect_true(rows_inserted > 0L)
  expect_equal(get_nrow(conn, test_table_name), rows_inserted)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs works with SELECT * in SQL string", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_select_star"
  test_origin_table <- "iris"
  
  # Create target table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0("SELECT * FROM ", test_origin_table, " WHERE 1 = 0")
  )
  
  # Query with SELECT *
  test_sql <- paste0("SELECT * FROM ", test_origin_table, " WHERE \"sepal.length\" > 7.0")
  
  rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql)
  
  expect_true(rows_inserted > 0L)
  expect_equal(get_nrow(conn, test_table_name), rows_inserted)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs works with aliases in SQL string", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_aliases"
  test_origin_table <- "iris"
  
  # Create target table
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0(
      "SELECT \"sepal.length\" AS sl, \"sepal.width\" AS sw ",
      "FROM ", test_origin_table, " WHERE 1 = 0"
    )
  )
  
  # Query with aliases
  test_sql <- paste0(
    "SELECT \"sepal.length\" AS sl, \"sepal.width\" AS sw ",
    "FROM ", test_origin_table, " WHERE \"sepal.length\" > 7.0"
  )
  
  rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql)
  
  expect_true(rows_inserted > 0L)
  expect_equal(get_nrow(conn, test_table_name), rows_inserted)
  
  dbRemoveTable(conn, test_table_name)
})

test_that("dbAppendTableAs respects auto_reorder = FALSE", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbappendtableas_no_reorder"
  test_origin_table <- "iris"
  
  # Create target table with specific column order
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  dbCreateTableAs(
    conn, test_table_name,
    paste0(
      "SELECT \"sepal.length\", \"sepal.width\" ",
      "FROM ", test_origin_table, " WHERE 1 = 0"
    )
  )
  
  # Query with different column order
  test_sql <- paste0(
    "SELECT \"sepal.width\", \"sepal.length\" ",
    "FROM ", test_origin_table, " LIMIT 1"
  )
  
  # Should not show reorder message when auto_reorder = FALSE
  # Note: This test may need adjustment based on Presto's actual behavior
  messages <- capture_messages(
    rows_inserted <- dbAppendTableAs(conn, test_table_name, sql = test_sql, auto_reorder = FALSE)
  )
  
  expect_false(any(grepl("Column order mismatch detected", messages)))
  expect_true(rows_inserted > 0L)
  
  dbRemoveTable(conn, test_table_name)
})

