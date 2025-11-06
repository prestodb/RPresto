# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbplyr-db"))

test_that("db_save_query works with new table", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery"
  test_origin_table <- "iris"
  test_sql <- paste0("SELECT * FROM ", test_origin_table)

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  expect_error(
    dplyr::db_save_query(conn, test_sql, test_table_name),
    "Temporary table is not supported"
  )

  result <- dplyr::db_save_query(
    conn, test_sql, test_table_name,
    temporary = FALSE
  )
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(
    dbListFields(conn, test_table_name),
    dbListFields(conn, test_origin_table)
  )
  expect_equal(
    get_nrow(conn, test_table_name),
    get_nrow(conn, test_origin_table)
  )

  dbRemoveTable(conn, test_table_name)
})

test_that("db_save_query fails when table exists and overwrite is FALSE", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery_overwrite"
  test_sql <- "SELECT * FROM iris"

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  result <- dplyr::db_save_query(
    conn, test_sql, test_table_name,
    temporary = FALSE
  )
  expect_true(dbExistsTable(conn, test_table_name))

  expect_error(
    dplyr::db_save_query(
      conn, test_sql, test_table_name,
      temporary = FALSE
    ),
    "The table .* exists but overwrite is set to FALSE"
  )

  dbRemoveTable(conn, test_table_name)
})

test_that("db_save_query overwrites table when overwrite is TRUE", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery_overwrite_success"
  test_origin_table <- "iris"
  test_sql <- paste0("SELECT * FROM ", test_origin_table)

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  result <- dplyr::db_save_query(
    conn, test_sql, test_table_name,
    temporary = FALSE
  )
  expect_true(dbExistsTable(conn, test_table_name))
  original_nrow <- get_nrow(conn, test_table_name)

  messages <- capture_messages(
    result <- dplyr::db_save_query(
      conn, test_sql, test_table_name,
      temporary = FALSE, overwrite = TRUE
    )
  )
  expect_true(any(grepl("Renaming existing table", messages)))
  expect_true(any(grepl("Dropping renamed table", messages)))
  expect_true(any(grepl("is overwritten", messages)))
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(get_nrow(conn, test_table_name), original_nrow)

  dbRemoveTable(conn, test_table_name)
})

test_that("db_save_query restores table on failure when overwrite is TRUE", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery_overwrite_failure"
  test_origin_table <- "iris"
  test_sql <- paste0("SELECT * FROM ", test_origin_table)

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  result <- dplyr::db_save_query(
    conn, test_sql, test_table_name,
    temporary = FALSE
  )
  expect_true(dbExistsTable(conn, test_table_name))
  original_nrow <- get_nrow(conn, test_table_name)

  invalid_sql <- "SELECT * FROM nonexistent_table"
  messages <- capture_messages(
    expect_error(
      dplyr::db_save_query(
        conn, invalid_sql, test_table_name,
        temporary = FALSE, overwrite = TRUE
      ),
      "Overwriting table .* failed"
    )
  )
  expect_true(any(grepl("Renaming existing table", messages)))
  expect_true(any(grepl("Reverting original table", messages)))

  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(get_nrow(conn, test_table_name), original_nrow)

  dbRemoveTable(conn, test_table_name)
})

test_that("db_save_query works with different table name formats", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery_formats"
  test_origin_table <- "iris"
  test_sql <- paste0("SELECT * FROM ", test_origin_table)

  # Test with character name
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  result <- dplyr::db_save_query(
    conn, test_sql, test_table_name,
    temporary = FALSE
  )
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))
  dbRemoveTable(conn, test_table_name)

  # Test with Id
  test_id <- DBI::Id(table = test_table_name)
  result <- dplyr::db_save_query(
    conn, test_sql, test_id,
    temporary = FALSE
  )
  expect_equal(result, test_id)
  expect_true(dbExistsTable(conn, test_table_name))
  dbRemoveTable(conn, test_table_name)

  # Test with in_schema
  test_schema <- dbplyr::in_schema(conn@schema, test_table_name)
  result <- dplyr::db_save_query(
    conn, test_sql, test_schema,
    temporary = FALSE
  )
  expect_equal(result, test_schema)
  expect_true(dbExistsTable(conn, test_table_name))
  dbRemoveTable(conn, test_table_name)

  # Test with dbQuoteIdentifier
  test_quoted <- DBI::dbQuoteIdentifier(conn, test_table_name)
  result <- dplyr::db_save_query(
    conn, test_sql, test_quoted,
    temporary = FALSE
  )
  expect_equal(result, test_quoted)
  expect_true(dbExistsTable(conn, test_table_name))
  dbRemoveTable(conn, test_table_name)
})

test_that("db_save_query overwrites with different data correctly", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery_overwrite_data"
  test_origin_table <- "iris"
  test_sql1 <- paste0("SELECT * FROM ", test_origin_table)
  test_sql2 <- paste0("SELECT * FROM ", test_origin_table, " LIMIT 10")

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  # Create initial table
  result <- dplyr::db_save_query(
    conn, test_sql1, test_table_name,
    temporary = FALSE
  )
  expect_true(dbExistsTable(conn, test_table_name))
  original_nrow <- get_nrow(conn, test_table_name)

  # Overwrite with different data
  messages <- capture_messages(
    result <- dplyr::db_save_query(
      conn, test_sql2, test_table_name,
      temporary = FALSE, overwrite = TRUE
    )
  )
  expect_true(any(grepl("Renaming existing table", messages)))
  expect_true(any(grepl("Dropping renamed table", messages)))
  expect_true(any(grepl("is overwritten", messages)))
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))
  new_nrow <- get_nrow(conn, test_table_name)
  expect_lt(new_nrow, original_nrow)
  expect_equal(new_nrow, 10)

  dbRemoveTable(conn, test_table_name)
})

test_that("db_save_query validates overwrite parameter", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbsavequery_validate"
  test_sql <- "SELECT * FROM iris"

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }

  # Test with invalid overwrite (not logical)
  expect_error(
    dplyr::db_save_query(
      conn, test_sql, test_table_name,
      temporary = FALSE, overwrite = "TRUE"
    )
  )

  # Test with invalid overwrite (NA)
  expect_error(
    dplyr::db_save_query(
      conn, test_sql, test_table_name,
      temporary = FALSE, overwrite = NA
    )
  )

  # Test with valid overwrite = FALSE
  result <- dplyr::db_save_query(
    conn, test_sql, test_table_name,
    temporary = FALSE, overwrite = FALSE
  )
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))

  dbRemoveTable(conn, test_table_name)
})

test_that("db_compute works with new table", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbcompute"
  test_origin_table <- "iris"
  test_sql <- paste0("SELECT * FROM ", test_origin_table)

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  expect_error(
    dbplyr::db_compute(conn, test_table_name, test_sql),
    "Temporary table is not supported"
  )

  result <- dbplyr::db_compute(
    conn, test_table_name, test_sql,
    temporary = FALSE
  )
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(
    dbListFields(conn, test_table_name),
    dbListFields(conn, test_origin_table)
  )
  expect_equal(
    get_nrow(conn, test_table_name),
    get_nrow(conn, test_origin_table)
  )

  dbRemoveTable(conn, test_table_name)
})

test_that("db_compute works with overwrite parameter", {
  conn <- setup_live_connection()
  test_table_name <- "test_dbcompute_overwrite"
  test_origin_table <- "iris"
  test_sql1 <- paste0("SELECT * FROM ", test_origin_table)
  test_sql2 <- paste0("SELECT * FROM ", test_origin_table, " LIMIT 10")

  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))

  # Create initial table
  result <- dbplyr::db_compute(
    conn, test_table_name, test_sql1,
    temporary = FALSE
  )
  expect_true(dbExistsTable(conn, test_table_name))
  original_nrow <- get_nrow(conn, test_table_name)

  # Overwrite with different data
  messages <- capture_messages(
    result <- dbplyr::db_compute(
      conn, test_table_name, test_sql2,
      temporary = FALSE, overwrite = TRUE
    )
  )
  expect_true(any(grepl("Renaming existing table", messages)))
  expect_true(any(grepl("Dropping renamed table", messages)))
  expect_true(any(grepl("is overwritten", messages)))
  expect_equal(result, test_table_name)
  expect_true(dbExistsTable(conn, test_table_name))
  new_nrow <- get_nrow(conn, test_table_name)
  expect_lt(new_nrow, original_nrow)
  expect_equal(new_nrow, 10)

  dbRemoveTable(conn, test_table_name)
})

