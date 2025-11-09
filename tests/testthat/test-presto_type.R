# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "presto_type"))

test_that("presto_type works with live database - basic types", {
  conn <- setup_live_connection()

  col_info <- dplyr::tbl(conn, "iris") %>%
    presto_type()
  expect_s3_class(col_info, "data.frame")
  expect_equal(nrow(col_info), 5)
  expect_equal(
    col_info$name,
    c(
      "sepal.length", "sepal.width", "petal.length",
      "petal.width", "species"
    )
  )
  expect_true(all(col_info$type %in% c("numeric", "character")))
  expect_true(all(col_info$.presto_type %in% c("DOUBLE", "VARCHAR")))
})

test_that("presto_type works with live database - array types", {
  conn <- setup_live_connection()

  # Create a test table with array types
  test_table <- "test_array_table"
  # Drop table if it exists from a previous test run
  tryCatch(
    dbRemoveTable(conn, test_table),
    error = function(e) NULL
  )
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT ARRAY[1, 2, 3] AS col_array_int,",
      "ARRAY['a', 'b'] AS col_array_varchar"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dplyr::tbl(conn, test_table) %>%
    presto_type()
  expect_equal(nrow(col_info), 2)
  expect_equal(
    col_info$name,
    c("col_array_int", "col_array_varchar")
  )
  expect_equal(col_info$type, c("list", "list"))
  expect_equal(
    col_info$.presto_type,
    c("ARRAY<INTEGER>", "ARRAY<VARCHAR>")
  )
})

test_that("presto_type works with live database - map types", {
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
    "SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]) AS col_map"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dplyr::tbl(conn, test_table) %>%
    presto_type()
  expect_equal(nrow(col_info), 1)
  expect_equal(col_info$name, "col_map")
  expect_equal(col_info$type, "list")
  expect_equal(col_info$.presto_type, "MAP<VARCHAR,INTEGER>")
})

test_that("presto_type works with live database - row types", {
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
    paste(
      "SELECT CAST(ROW(1, 'text') AS",
      "ROW(f1 INTEGER, f2 VARCHAR)) AS col_row"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dplyr::tbl(conn, test_table) %>%
    presto_type()
  expect_equal(nrow(col_info), 1)
  expect_equal(col_info$name, "col_row")
  expect_equal(col_info$type, "list")
  expect_equal(
    col_info$.presto_type,
    "ROW(f1 INTEGER, f2 VARCHAR)"
  )
})

test_that("presto_type works with queries", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_query_table"
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT 1 AS id, 'text' AS name, ARRAY[1, 2, 3] AS arr"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Test with a query (filtered table)
  col_info <- dplyr::tbl(conn, test_table) %>%
    dplyr::filter(id > 0) %>%
    presto_type()
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("id", "name", "arr"))
  expect_equal(col_info$type, c("integer", "character", "list"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "VARCHAR", "ARRAY<INTEGER>")
  )
})

test_that("presto_type works with nested complex types", {
  conn <- setup_live_connection()

  # Create a test table with nested complex types
  test_table <- "test_nested_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT",
      "ARRAY[MAP(ARRAY['a'], ARRAY[1])] AS col_array_map,",
      "ARRAY[CAST(ROW(1) AS ROW(f1 INTEGER))] AS col_array_row,",
      "CAST(ROW(ARRAY[1, 2]) AS ROW(f1 ARRAY<INTEGER>)) AS col_row_array,",
      paste(
        "CAST(ROW(MAP(ARRAY['a'], ARRAY[1])) AS",
        "ROW(f1 MAP<VARCHAR,INTEGER>)) AS col_row_map"
      )
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  col_info <- dplyr::tbl(conn, test_table) %>%
    presto_type()
  expect_equal(nrow(col_info), 4)
  expect_equal(
    col_info$name,
    c(
      "col_array_map", "col_array_row", "col_row_array",
      "col_row_map"
    )
  )
  expect_equal(col_info$type, rep("list", 4))
  expect_equal(
    col_info$.presto_type,
    c(
      "ARRAY<MAP<VARCHAR,INTEGER>>",
      "ARRAY<ROW(f1 INTEGER)>",
      "ROW(f1 ARRAY<INTEGER>)",
      "ROW(f1 MAP<VARCHAR,INTEGER>)"
    )
  )
})

test_that("presto_type errors on ARRAY of ARRAY", {
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
    dplyr::tbl(conn, test_table) %>%
      presto_type(),
    "ARRAY of ARRAY"
  )
})

test_that("presto_type works with filter operation", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_filter_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT 1 AS id, 'text' AS name,",
      "CAST(10.5 AS DOUBLE) AS value"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Test with filter operation
  col_info <- dplyr::tbl(conn, test_table) %>%
    dplyr::filter(id > 0) %>%
    presto_type()
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("id", "name", "value"))
  expect_equal(col_info$type, c("integer", "character", "numeric"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "VARCHAR", "DOUBLE")
  )
})

test_that("presto_type works with select operation", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_select_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT 1 AS id, 'text' AS name,",
      "CAST(10.5 AS DOUBLE) AS value, ARRAY[1, 2] AS arr"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Test with select operation (subset of columns)
  col_info <- dplyr::tbl(conn, test_table) %>%
    dplyr::select(id, arr) %>%
    presto_type()
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("id", "arr"))
  expect_equal(col_info$type, c("integer", "list"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "ARRAY<INTEGER>")
  )
})

test_that("presto_type works with mutate operation", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_mutate_table"
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT 1 AS id, CAST(10 AS DOUBLE) AS value"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Test with mutate operation (add new column)
  col_info <- dplyr::tbl(conn, test_table) %>%
    dplyr::mutate(value_doubled = value * 2) %>%
    presto_type()
  expect_equal(nrow(col_info), 3)
  expect_equal(
    col_info$name,
    c("id", "value", "value_doubled")
  )
  expect_equal(col_info$type, c("integer", "numeric", "numeric"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "DOUBLE", "DOUBLE")
  )
})

test_that("presto_type works with group_by and summarize", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_group_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT 1 AS id, 'A' AS category,",
      "CAST(10 AS DOUBLE) AS value",
      "UNION ALL SELECT 2, 'A', CAST(20 AS DOUBLE)",
      "UNION ALL SELECT 3, 'B', CAST(30 AS DOUBLE)"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Test with group_by and summarize
  col_info <- dplyr::tbl(conn, test_table) %>%
    dplyr::group_by(category) %>%
    dplyr::summarize(avg_value = mean(value, na.rm = TRUE)) %>%
    presto_type()
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("category", "avg_value"))
  expect_equal(col_info$type, c("character", "numeric"))
  expect_equal(
    col_info$.presto_type,
    c("VARCHAR", "DOUBLE")
  )
})

test_that("presto_type works with arrange operation", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_arrange_table"
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT 1 AS id, 'text' AS name"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Test with arrange operation
  col_info <- dplyr::tbl(conn, test_table) %>%
    dplyr::arrange(id) %>%
    presto_type()
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("id", "name"))
  expect_equal(col_info$type, c("integer", "character"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "VARCHAR")
  )
})

test_that("presto_type works with join operation", {
  conn <- setup_live_connection()

  # Create test tables
  test_table1 <- "test_join_table1"
  test_table2 <- "test_join_table2"
  dbCreateTableAs(
    conn,
    test_table1,
    "SELECT 1 AS id, 'A' AS name"
  )
  dbCreateTableAs(
    conn,
    test_table2,
    "SELECT 1 AS id, CAST(10 AS DOUBLE) AS value"
  )
  on.exit({
    dbRemoveTable(conn, test_table1)
    dbRemoveTable(conn, test_table2)
  }, add = TRUE)

  # Test with inner join
  col_info <- dplyr::tbl(conn, test_table1) %>%
    dplyr::inner_join(
      dplyr::tbl(conn, test_table2),
      by = "id"
    ) %>%
    presto_type()
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("id", "name", "value"))
  expect_equal(col_info$type, c("integer", "character", "numeric"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "VARCHAR", "DOUBLE")
  )
})

test_that("presto_type works with CTE", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_cte_table"
  dbCreateTableAs(
    conn,
    test_table,
    "SELECT 1 AS id, 'text' AS name, ARRAY[1, 2, 3] AS arr"
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Create a CTE using compute()
  tbl_cte <- dplyr::tbl(conn, test_table) %>%
    dplyr::filter(id > 0) %>%
    dplyr::compute("test_cte", cte = TRUE)
  on.exit(
    {
      if (conn@session$hasCTE("test_cte")) {
        conn@session$removeCTE("test_cte")
      }
    },
    add = TRUE
  )

  # Test presto_type on CTE
  col_info <- tbl_cte %>%
    presto_type()
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("id", "name", "arr"))
  expect_equal(col_info$type, c("integer", "character", "list"))
  expect_equal(
    col_info$.presto_type,
    c("INTEGER", "VARCHAR", "ARRAY<INTEGER>")
  )
})

test_that("presto_type works with nested CTEs", {
  conn <- setup_live_connection()

  # Create a test table
  test_table <- "test_nested_cte_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT 1 AS id, 'A' AS category,",
      "CAST(10 AS DOUBLE) AS value",
      "UNION ALL SELECT 2, 'A', CAST(20 AS DOUBLE)",
      "UNION ALL SELECT 3, 'B', CAST(30 AS DOUBLE)"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Create first CTE
  tbl_cte1 <- dplyr::tbl(conn, test_table) %>%
    dplyr::group_by(category) %>%
    dplyr::summarize(avg_value = mean(value, na.rm = TRUE)) %>%
    dplyr::compute("cte1", cte = TRUE)
  on.exit(
    {
      if (conn@session$hasCTE("cte1")) {
        conn@session$removeCTE("cte1")
      }
    },
    add = TRUE
  )

  # Create second CTE that uses first CTE
  # avg_value is already DOUBLE from mean(), so avg_value * 2
  # should also be DOUBLE
  tbl_cte2 <- tbl_cte1 %>%
    dplyr::mutate(total = avg_value * 2) %>%
    dplyr::compute("cte2", cte = TRUE)
  on.exit(
    {
      if (conn@session$hasCTE("cte2")) {
        conn@session$removeCTE("cte2")
      }
    },
    add = TRUE
  )

  # Test presto_type on nested CTE
  col_info <- tbl_cte2 %>%
    presto_type()
  expect_equal(nrow(col_info), 3)
  expect_equal(col_info$name, c("category", "avg_value", "total"))
  expect_equal(col_info$type, c("character", "numeric", "numeric"))
  expect_equal(
    col_info$.presto_type,
    c("VARCHAR", "DOUBLE", "DOUBLE")
  )
})

test_that("presto_type works with CTE and complex types", {
  conn <- setup_live_connection()

  # Create a test table with complex types
  test_table <- "test_cte_complex_table"
  dbCreateTableAs(
    conn,
    test_table,
    paste(
      "SELECT MAP(ARRAY['a', 'b'], ARRAY[1, 2]) AS col_map,",
      "CAST(ROW(1, 'text') AS",
      "ROW(f1 INTEGER, f2 VARCHAR)) AS col_row"
    )
  )
  on.exit(dbRemoveTable(conn, test_table), add = TRUE)

  # Create a CTE (need to add an operation before compute)
  tbl_cte <- dplyr::tbl(conn, test_table) %>%
    dplyr::filter(TRUE) %>%
    dplyr::compute("test_cte_complex", cte = TRUE)
  on.exit(
    {
      if (conn@session$hasCTE("test_cte_complex")) {
        conn@session$removeCTE("test_cte_complex")
      }
    },
    add = TRUE
  )

  # Test presto_type on CTE with complex types
  col_info <- tbl_cte %>%
    presto_type()
  expect_equal(nrow(col_info), 2)
  expect_equal(col_info$name, c("col_map", "col_row"))
  expect_equal(col_info$type, c("list", "list"))
  expect_equal(
    col_info$.presto_type,
    c("MAP<VARCHAR,INTEGER>", "ROW(f1 INTEGER, f2 VARCHAR)")
  )
})

