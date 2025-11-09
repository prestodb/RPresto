# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbColumnInfo - PrestoResult"))

test_that("dbColumnInfo works with live database - basic types", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    paste0(
      "SELECT 1 AS col_int, CAST(1.5 AS DOUBLE) AS col_double, ",
      "'text' AS col_varchar, true AS col_boolean LIMIT 1"
    )
  )
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_s3_class(col_info, "data.frame")
  expect_equal(nrow(col_info), 4)
  expect_equal(col_info$name, c("col_int", "col_double", "col_varchar", "col_boolean"))
  expect_equal(col_info$type, c("integer", "numeric", "character", "logical"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "DOUBLE", "VARCHAR", "BOOLEAN")
  )

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo works with live database - array types", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    "SELECT ARRAY[1, 2, 3] AS col_array_int, ARRAY['a', 'b'] AS col_array_varchar LIMIT 1"
  )
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("col_array_int", "col_array_varchar"))
  expect_equal(col_info$type, c("list", "list"))
  expect_equal(
    col_info$.presto_type,
    c("ARRAY<INTEGER>", "ARRAY<VARCHAR>")
  )

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo works with live database - map types", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    "SELECT MAP(ARRAY['k1', 'k2'], ARRAY[1, 2]) AS col_map LIMIT 1"
  )
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_equal(nrow(col_info), 1)
  expect_equal(col_info$name, "col_map")
  expect_equal(col_info$type, "list")
  expect_equal(col_info$.presto_type, "MAP<VARCHAR,INTEGER>")

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo works with live database - row types", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    paste0(
      "SELECT CAST(ROW(1, 'text', 1.5) AS ",
      "ROW(field1 INTEGER, field2 VARCHAR, field3 DOUBLE)) AS col_row LIMIT 1"
    )
  )
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_equal(nrow(col_info), 1)
  expect_equal(col_info$name, "col_row")
  expect_equal(col_info$type, "list")
  expect_true(
    grepl("ROW\\(field1 INTEGER, field2 VARCHAR, field3 DOUBLE\\)", col_info$.presto_type)
  )

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo works with live database - date and time types", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    paste0(
      "SELECT DATE '2023-01-01' AS col_date, ",
      "TIMESTAMP '2023-01-01 12:00:00' AS col_timestamp, ",
      "TIME '12:00:00' AS col_time LIMIT 1"
    )
  )
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("col_date", "col_timestamp", "col_time"))
  expect_equal(col_info$type, c("Date", "POSIXct", "difftime"))
  expect_equal(
    col_info$.presto_type,
    c("DATE", "TIMESTAMP", "TIME")
  )

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo works with live database - mixed types", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    paste0(
      "SELECT 1 AS int_col, ARRAY[1, 2] AS array_col, ",
      "MAP(ARRAY['k'], ARRAY[1]) AS map_col, ",
      "CAST(ROW(1, 'text') AS ROW(f1 INTEGER, f2 VARCHAR)) AS row_col LIMIT 1"
    )
  )
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_equal(nrow(col_info), 4)
  expect_equal(col_info$name, c("int_col", "array_col", "map_col", "row_col"))
  expect_equal(col_info$type, c("integer", "list", "list", "list"))
  expect_equal(col_info$.presto_type[1], "INTEGER")
  expect_equal(col_info$.presto_type[2], "ARRAY<INTEGER>")
  expect_equal(col_info$.presto_type[3], "MAP<VARCHAR,INTEGER>")
  expect_true(grepl("ROW\\(f1 INTEGER, f2 VARCHAR\\)", col_info$.presto_type[4]))

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo works with live database - nested complex types", {
  conn <- setup_live_connection()

  # Test: ARRAY of MAP
  result1 <- dbSendQuery(
    conn,
    paste0(
      "SELECT ARRAY[MAP(ARRAY['k1'], ARRAY[1]), ",
      "MAP(ARRAY['k2'], ARRAY[2])] AS col_array_map LIMIT 1"
    )
  )
  dbFetch(result1, n = -1)
  col_info1 <- dbColumnInfo(result1)
  expect_equal(col_info1$name, "col_array_map")
  expect_equal(col_info1$type, "list")
  expect_equal(col_info1$.presto_type, "ARRAY<MAP<VARCHAR,INTEGER>>")
  expect_true(dbClearResult(result1))

  # Test: ARRAY of ROW
  result2 <- dbSendQuery(
    conn,
    paste0(
      "SELECT ARRAY[CAST(ROW(1, 'text') AS ROW(f1 INTEGER, f2 VARCHAR)), ",
      "CAST(ROW(2, 'text2') AS ROW(f1 INTEGER, f2 VARCHAR))] AS col_array_row LIMIT 1"
    )
  )
  dbFetch(result2, n = -1)
  col_info2 <- dbColumnInfo(result2)
  expect_equal(col_info2$name, "col_array_row")
  expect_equal(col_info2$type, "list")
  expect_true(
    grepl("ARRAY<ROW\\(f1 INTEGER, f2 VARCHAR\\)>", col_info2$.presto_type)
  )
  expect_true(dbClearResult(result2))

  # Test: ROW with ARRAY field
  result3 <- dbSendQuery(
    conn,
    paste0(
      "SELECT CAST(ROW(1, ARRAY[1, 2]) AS ",
      "ROW(f1 INTEGER, f2 ARRAY<INTEGER>)) AS col_row_array LIMIT 1"
    )
  )
  dbFetch(result3, n = -1)
  col_info3 <- dbColumnInfo(result3)
  expect_equal(col_info3$name, "col_row_array")
  expect_equal(col_info3$type, "list")
  expect_equal(
    col_info3$.presto_type,
    "ROW(f1 INTEGER, f2 ARRAY<INTEGER>)"
  )
  expect_true(dbClearResult(result3))

  # Test: ROW with MAP field
  result4 <- dbSendQuery(
    conn,
    paste0(
      "SELECT CAST(ROW(1, MAP(ARRAY['k'], ARRAY[1])) AS ",
      "ROW(f1 INTEGER, f2 MAP<VARCHAR,INTEGER>)) AS col_row_map LIMIT 1"
    )
  )
  dbFetch(result4, n = -1)
  col_info4 <- dbColumnInfo(result4)
  expect_equal(col_info4$name, "col_row_map")
  expect_equal(col_info4$type, "list")
  # Presto formats as MAP<VARCHAR,INTEGER> (with angle brackets)
  expect_equal(
    col_info4$.presto_type,
    "ROW(f1 INTEGER, f2 MAP<VARCHAR,INTEGER>)"
  )
  expect_true(dbClearResult(result4))

  # Test: MAP with nested MAP
  result5 <- dbSendQuery(
    conn,
    paste0(
      "SELECT MAP(ARRAY['k1'], ",
      "ARRAY[MAP(ARRAY['k2'], ARRAY[1])]) AS col_map_map LIMIT 1"
    )
  )
  dbFetch(result5, n = -1)
  col_info5 <- dbColumnInfo(result5)
  expect_equal(col_info5$name, "col_map_map")
  expect_equal(col_info5$type, "list")
  expect_equal(col_info5$.presto_type, "MAP<VARCHAR,MAP<VARCHAR,INTEGER>>")
  expect_true(dbClearResult(result5))
})

test_that("dbColumnInfo works with fallback mechanism", {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, "SELECT 1 AS col1, 2 AS col2")
  # Don't fetch - test fallback mechanism

  col_info <- dbColumnInfo(result)
  expect_s3_class(col_info, "data.frame")
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("col1", "col2"))
  expect_equal(col_info$type, c("integer", "integer"))
  expect_equal(col_info$.presto_type, c("INTEGER", "INTEGER"))

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo errors on ARRAY of ARRAY", {
  conn <- setup_live_connection()

  result <- dbSendQuery(
    conn,
    "SELECT ARRAY[ARRAY[1, 2], ARRAY[3, 4]] AS col_array_array LIMIT 1"
  )

  expect_error(
    dbColumnInfo(result),
    "The field \\[col_array_array\\] is an ARRAY of ARRAY which is not supported"
  )

  expect_true(dbClearResult(result))
})

test_that("dbColumnInfo errors on invalid result", {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, "SELECT 1 AS col1")
  expect_true(dbClearResult(result))

  expect_error(
    dbColumnInfo(result),
    "The result object is not valid"
  )
})

test_that("dbColumnInfo returns correct structure", {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, "SELECT 1 AS col1, 2 AS col2")
  dbFetch(result, n = -1)

  col_info <- dbColumnInfo(result)
  expect_s3_class(col_info, "data.frame")
  expect_equal(colnames(col_info), c("name", "type", ".presto_type"))
  expect_true(all(sapply(col_info, is.character)))

  expect_true(dbClearResult(result))
})

