# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(
  Sys.getenv("PRESTO_TYPE", "Presto"),
  "presto_unnest"
))

test_that("unnest works with arrays", {
  conn <- setup_live_connection()

  test_table <- "unnest_arr_test"
  # Drop table if it exists from a previous test run
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20, 30]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  tbl_out <- tbl_in %>%
    presto_unnest(arr, values_to = "elem")
  expect_equal(dplyr::pull(dplyr::tally(tbl_out), n), 4L)
  out <- tbl_out %>%
    dplyr::collect()

  expect_equal(nrow(out), 4L)
  expect_equal(names(out), c("id", "arr", "elem"))
  # Values expanded correctly
  expect_equal(sort(out$elem), c(5L, 10L, 20L, 30L))
})

test_that("unnest without values_to uses column name with _elem suffix", {
  conn <- setup_live_connection()

  test_table <- "unnest_no_values_to_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  # Test without values_to - should use arr_elem as default
  tbl_out <- tbl_in %>%
    presto_unnest(arr)
  out <- tbl_out %>%
    dplyr::collect()

  # Should have id, arr (original), and arr_elem (unnested)
  expect_equal(nrow(out), 3L)
  expect_equal(names(out), c("id", "arr", "arr_elem"))
  expect_equal(sort(out$arr_elem), c(5L, 10L, 20L))
})

test_that("unnest supports WITH ORDINALITY", {
  conn <- setup_live_connection()

  test_table <- "unnest_arr_ord_test"
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(VARCHAR))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY['a','b','c'])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  tbl_out <- tbl_in %>%
    presto_unnest(
      arr,
      values_to = "val",
      with_ordinality = TRUE,
      ordinality_to = "idx"
    )
  out <- dplyr::collect(tbl_out)

  expect_equal(nrow(out), 3L)
  expect_equal(names(out), c("id", "arr", "val", "idx"))
  expect_equal(sort(out$idx), c(1L, 2L, 3L))
})


test_that("unnest works on subqueries", {
  conn <- setup_live_connection()

  test_table <- "unnest_arr_subq_test"
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20, 30]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  subq <- tbl_in %>%
    dplyr::filter(id == 1L) %>%
    dplyr::select(id, arr)

  tbl_out <- subq %>%
    presto_unnest(arr, values_to = "elem")
  out <- dplyr::collect(tbl_out)

  expect_equal(nrow(out), 3L)
  expect_equal(names(out), c("id", "arr", "elem"))
  expect_equal(sort(out$elem), c(10L, 20L, 30L))
})

test_that("unnest works when source is a CTE", {
  conn <- setup_live_connection()

  test_table <- "unnest_arr_cte_src"
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[1, 2]), (2, ARRAY[3, 4, 5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  # Make a non-trivial change (rename), so compute() has work to do
  cte_rel <- tbl_in %>%
    dplyr::rename(arr2 = arr) %>%
    dplyr::compute(name = "unnest_arr_cte", cte = TRUE)

  tbl_out <- cte_rel %>%
    presto_unnest(arr2, values_to = "elem")
  out <- dplyr::collect(tbl_out)

  expect_equal(names(out), c("id", "arr2", "elem"))
  expect_equal(sort(out$elem), c(1L, 2L, 3L, 4L, 5L))
})

test_that("unnest works with group_by", {
  conn <- setup_live_connection()

  test_table <- "unnest_grps_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  tbl_out <- tbl_in %>%
    dplyr::group_by(id) %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::summarize(count = dplyr::n())
  out <- dplyr::collect(tbl_out)

  expect_equal(nrow(out), 2L)
  expect_equal(names(out), c("id", "count"))
  expect_equal(sort(out$count), c(1L, 2L))
})

test_that("unnest works with arrange", {
  conn <- setup_live_connection()

  test_table <- "unnest_sort_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (2, ARRAY[5]), (1, ARRAY[10, 20])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  tbl_out <- tbl_in %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::arrange(id, elem)
  out <- dplyr::collect(tbl_out)

  expect_equal(nrow(out), 3L)
  expect_equal(out$id, c(1L, 1L, 2L))
  expect_equal(out$elem, c(10L, 20L, 5L))
})

test_that("unnest works with window functions", {
  conn <- setup_live_connection()

  test_table <- "unnest_frame_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  tbl_out <- tbl_in %>%
    dplyr::mutate(row_num = dplyr::row_number()) %>%
    presto_unnest(arr, values_to = "elem")
  out <- dplyr::collect(tbl_out)

  expect_equal(nrow(out), 3L)
  expect_equal(names(out), c("id", "arr", "row_num", "elem"))
  expect_true(all(out$row_num %in% c(1L, 2L)))
})

test_that("unnest followed by group_by and summarize works correctly", {
  conn <- setup_live_connection()

  test_table <- "unnest_then_group_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  # This is the problematic sequence: unnest THEN group_by THEN summarize
  tbl_out <- tbl_in %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::group_by(id) %>%
    dplyr::summarize(count = dplyr::n())
  out <- dplyr::collect(tbl_out)

  # Expected: 2 rows (one for each id), with counts 2 and 1
  expect_equal(nrow(out), 2L)
  expect_equal(names(out), c("id", "count"))
  expect_equal(sort(out$count), c(1L, 2L))
  # Verify id=1 has count=2 (two elements in array), id=2 has count=1
  expect_equal(out$count[out$id == 1L], 2L)
  expect_equal(out$count[out$id == 2L], 1L)
})

test_that("unnest followed by group_by and summarize works with CTE", {
  conn <- setup_live_connection()

  test_table <- "unnest_then_group_cte_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  # This sequence works: unnest, save to CTE, THEN group_by and summarize
  cte_rel <- tbl_in %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::compute(name = "unnest_cte", cte = TRUE)

  tbl_out <- cte_rel %>%
    dplyr::group_by(id) %>%
    dplyr::summarize(count = dplyr::n())
  out <- dplyr::collect(tbl_out)

  # Expected: 2 rows (one for each id), with counts 2 and 1
  expect_equal(nrow(out), 2L)
  expect_equal(names(out), c("id", "count"))
  expect_equal(sort(out$count), c(1L, 2L))
  # Verify id=1 has count=2 (two elements in array), id=2 has count=1
  expect_equal(out$count[out$id == 1L], 2L)
  expect_equal(out$count[out$id == 2L], 1L)
})

test_that("operation -> CTE -> presto_unnest -> CTE works correctly", {
  conn <- setup_live_connection()

  test_table <- "unnest_op_cte_unnest_cte_test"
  tryCatch(
    DBI::dbExecute(conn, sprintf(
      "DROP TABLE IF EXISTS %s",
      DBI::dbQuoteIdentifier(conn, test_table)
    )),
    error = function(e) NULL
  )
  DBI::dbExecute(conn, sprintf(
    "CREATE TABLE %s (id BIGINT, arr ARRAY(BIGINT))",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))
  on.exit(DBI::dbRemoveTable(conn, test_table), add = TRUE)

  DBI::dbExecute(conn, sprintf(
    "INSERT INTO %s VALUES (1, ARRAY[10, 20]), (2, ARRAY[5])",
    DBI::dbQuoteIdentifier(conn, test_table)
  ))

  tbl_in <- dplyr::tbl(conn, test_table)
  # Step 1: Apply operation (filter) -> CTE
  cte1 <- tbl_in %>%
    dplyr::filter(id == 1L) %>%
    dplyr::compute(name = "filtered_cte", cte = TRUE)
  on.exit(
    {
      if (conn@session$hasCTE("filtered_cte")) {
        conn@session$removeCTE("filtered_cte")
      }
    },
    add = TRUE
  )

  # Step 2: presto_unnest on CTE -> CTE
  cte2 <- cte1 %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::compute(name = "unnest_cte", cte = TRUE)
  on.exit(
    {
      if (conn@session$hasCTE("unnest_cte")) {
        conn@session$removeCTE("unnest_cte")
      }
    },
    add = TRUE
  )

  # Step 3: Verify the final CTE works correctly
  out <- dplyr::collect(cte2)

  expect_equal(nrow(out), 2L)
  expect_equal(names(out), c("id", "arr", "elem"))
  expect_equal(sort(out$elem), c(10L, 20L))
  expect_equal(unique(out$id), 1L)
})


