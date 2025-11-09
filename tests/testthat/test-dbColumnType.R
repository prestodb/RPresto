# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbColumnType"))

test_that("dbColumnType works with live database - basic types", {
  conn <- setup_live_connection()

  col_info <- dbColumnType(conn, "iris")
  expect_s3_class(col_info, "data.frame")
  expect_equal(nrow(col_info), 5)
  expect_equal(
    col_info$name,
    c("sepal.length", "sepal.width", "petal.length", "petal.width", "species")
  )
  expect_true(all(col_info$type %in% c("numeric", "character")))
  expect_true(all(col_info$.presto_type %in% c("DOUBLE", "VARCHAR")))
})

test_that("dbColumnType works with live database - array types", {
  conn <- setup_live_connection()

  # Create a test table with array types
  test_table <- "test_array_table"
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT ARRAY[1, 2, 3] AS col_array_int, ARRAY['a', 'b'] AS col_array_varchar"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dbColumnType(conn, test_table)
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("col_array_int", "col_array_varchar"))
  expect_equal(col_info$type, c("list", "list"))
  expect_equal(
    col_info$.presto_type,
    c("ARRAY<INTEGER>", "ARRAY<VARCHAR>")
  )
})

test_that("dbColumnType works with live database - map types", {
  conn <- setup_live_connection()

  # Create a test table with map types
  test_table <- "test_map_table"
  # Drop table if it exists from a previous test run
  tryCatch(
    dbRemoveTable(conn, test_table),
    error = function(e) NULL
  )
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT MAP(ARRAY['k1', 'k2'], ARRAY[1, 2]) AS col_map"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dbColumnType(conn, test_table)
  expect_equal(nrow(col_info), 1)
  expect_equal(col_info$name, "col_map")
  expect_equal(col_info$type, "list")
  expect_equal(col_info$.presto_type, "MAP<VARCHAR,INTEGER>")
})

test_that("dbColumnType works with live database - row types", {
  conn <- setup_live_connection()

  # Create a test table with row types
  test_table <- "test_row_table"
  # Drop table if it exists from a previous test run
  tryCatch(
    dbRemoveTable(conn, test_table),
    error = function(e) NULL
  )
  dbCreateTableAs(
    conn,
    test_table,
    paste0(
      "SELECT CAST(ROW(1, 'text', 1.5) AS ",
      "ROW(field1 INTEGER, field2 VARCHAR, field3 DOUBLE)) AS col_row"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dbColumnType(conn, test_table)
  expect_equal(nrow(col_info), 1)
  expect_equal(col_info$name, "col_row")
  expect_equal(col_info$type, "list")
  expect_true(
    grepl("ROW\\(field1 INTEGER, field2 VARCHAR, field3 DOUBLE\\)", col_info$.presto_type)
  )
})

test_that("dbColumnType works with live database - date and time types", {
  conn <- setup_live_connection()

  # Create a test table with date and time types
  test_table <- "test_date_time_table"
  # Drop table if it exists from a previous test run
  tryCatch(
    dbRemoveTable(conn, test_table),
    error = function(e) NULL
  )
  dbCreateTableAs(
    conn,
    test_table,
    paste0(
      "SELECT DATE '2023-01-01' AS col_date, ",
      "TIMESTAMP '2023-01-01 12:00:00' AS col_timestamp, ",
      "TIME '12:00:00' AS col_time"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dbColumnType(conn, test_table)
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("col_date", "col_timestamp", "col_time"))
  expect_equal(col_info$type, c("Date", "POSIXct", "difftime"))
  expect_equal(
    col_info$.presto_type,
    c("DATE", "TIMESTAMP", "TIME")
  )
})

test_that("dbColumnType works with live database - mixed types", {
  conn <- setup_live_connection()

  # Create a test table with mixed types
  test_table <- "test_mixed_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste0(
      "SELECT 1 AS int_col, ARRAY[1, 2] AS array_col, ",
      "MAP(ARRAY['k'], ARRAY[1]) AS map_col, ",
      "CAST(ROW(1, 'text') AS ROW(f1 INTEGER, f2 VARCHAR)) AS row_col"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dbColumnType(conn, test_table)
  expect_equal(nrow(col_info), 4)
  expect_equal(col_info$name, c("int_col", "array_col", "map_col", "row_col"))
  expect_equal(col_info$type, c("integer", "list", "list", "list"))
  expect_equal(col_info$.presto_type[1], "INTEGER")
  expect_equal(col_info$.presto_type[2], "ARRAY<INTEGER>")
  expect_equal(col_info$.presto_type[3], "MAP<VARCHAR,INTEGER>")
  expect_true(grepl("ROW\\(f1 INTEGER, f2 VARCHAR\\)", col_info$.presto_type[4]))
})

test_that("dbColumnType works with live database - nested complex types", {
  conn <- setup_live_connection()

  # Test: ARRAY of MAP
  test_table1 <- "test_array_map"
  dbCreateTableAs(
    conn,
    test_table1,
    paste0(
      "SELECT ARRAY[MAP(ARRAY['k1'], ARRAY[1]), ",
      "MAP(ARRAY['k2'], ARRAY[2])] AS col_array_map"
    )
  )
  on.exit(dbRemoveTable(conn, test_table1), add = TRUE)

  col_info1 <- dbColumnType(conn, test_table1)
  expect_equal(col_info1$name, "col_array_map")
  expect_equal(col_info1$type, "list")
  expect_equal(col_info1$.presto_type, "ARRAY<MAP<VARCHAR,INTEGER>>")

  # Test: ARRAY of ROW
  test_table2 <- "test_array_row"
  dbCreateTableAs(
    conn,
    test_table2,
    paste0(
      "SELECT ARRAY[CAST(ROW(1, 'text') AS ROW(f1 INTEGER, f2 VARCHAR)), ",
      "CAST(ROW(2, 'text2') AS ROW(f1 INTEGER, f2 VARCHAR))] AS col_array_row"
    )
  )
  on.exit(dbRemoveTable(conn, test_table2), add = TRUE)

  col_info2 <- dbColumnType(conn, test_table2)
  expect_equal(col_info2$name, "col_array_row")
  expect_equal(col_info2$type, "list")
  expect_true(
    grepl("ARRAY<ROW\\(f1 INTEGER, f2 VARCHAR\\)>", col_info2$.presto_type)
  )

  # Test: ROW with ARRAY field
  test_table3 <- "test_row_array"
  dbCreateTableAs(
    conn,
    test_table3,
    paste0(
      "SELECT CAST(ROW(1, ARRAY[1, 2]) AS ",
      "ROW(f1 INTEGER, f2 ARRAY<INTEGER>)) AS col_row_array"
    )
  )
  on.exit(dbRemoveTable(conn, test_table3), add = TRUE)

  col_info3 <- dbColumnType(conn, test_table3)
  expect_equal(col_info3$name, "col_row_array")
  expect_equal(col_info3$type, "list")
  expect_equal(
    col_info3$.presto_type,
    "ROW(f1 INTEGER, f2 ARRAY<INTEGER>)"
  )

  # Test: ROW with MAP field
  test_table4 <- "test_row_map"
  dbCreateTableAs(
    conn,
    test_table4,
    paste0(
      "SELECT CAST(ROW(1, MAP(ARRAY['k'], ARRAY[1])) AS ",
      "ROW(f1 INTEGER, f2 MAP<VARCHAR,INTEGER>)) AS col_row_map"
    )
  )
  on.exit(dbRemoveTable(conn, test_table4), add = TRUE)

  col_info4 <- dbColumnType(conn, test_table4)
  expect_equal(col_info4$name, "col_row_map")
  expect_equal(col_info4$type, "list")
  expect_equal(
    col_info4$.presto_type,
    "ROW(f1 INTEGER, f2 MAP<VARCHAR,INTEGER>)"
  )

  # Test: MAP with nested MAP
  test_table5 <- "test_map_map"
  dbCreateTableAs(
    conn,
    test_table5,
    paste0(
      "SELECT MAP(ARRAY['k1'], ",
      "ARRAY[MAP(ARRAY['k2'], ARRAY[1])]) AS col_map_map"
    )
  )
  on.exit(dbRemoveTable(conn, test_table5), add = TRUE)

  col_info5 <- dbColumnType(conn, test_table5)
  expect_equal(col_info5$name, "col_map_map")
  expect_equal(col_info5$type, "list")
  expect_equal(col_info5$.presto_type, "MAP<VARCHAR,MAP<VARCHAR,INTEGER>>")
})

test_that("dbColumnType errors on ARRAY of ARRAY", {
  conn <- setup_live_connection()

  # Create a test table with ARRAY of ARRAY
  test_table <- "test_array_array"
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] AS col_array_array"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # The error will come from extractData() when dbFetch() is called
  expect_error(
    dbColumnType(conn, test_table),
    "ARRAY of ARRAY"
  )
})

test_that("dbColumnType works with identifier", {
  conn <- setup_live_connection()

  col_info <- dbColumnType(conn, DBI::Id(table = "iris"))
  expect_s3_class(col_info, "data.frame")
  expect_equal(nrow(col_info), 5)

  col_info2 <- dbColumnType(conn, dbplyr::in_schema(conn@schema, "iris"))
  expect_s3_class(col_info2, "data.frame")
  expect_equal(nrow(col_info2), 5)

  col_info3 <- dbColumnType(conn, DBI::dbQuoteIdentifier(conn, "iris"))
  expect_s3_class(col_info3, "data.frame")
  expect_equal(nrow(col_info3), 5)
})

test_that("dbColumnType returns correct structure", {
  conn <- setup_live_connection()

  col_info <- dbColumnType(conn, "iris")
  expect_s3_class(col_info, "data.frame")
  expect_equal(colnames(col_info), c("name", "type", ".presto_type"))
  expect_true(all(sapply(col_info, is.character)))
})

test_that("dbColumnType errors on non-existent table", {
  conn <- setup_live_connection()

  expect_error(
    dbColumnType(conn, "__non_existent_table__"),
    "Table.*does not exist"
  )
})

