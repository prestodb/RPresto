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
  out <- tbl_in %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::collect()

  expect_equal(nrow(out), 4L)
  expect_equal(names(out), c("id", "arr", "elem"))
  # Values expanded correctly
  expect_equal(sort(out$elem), c(5L, 10L, 20L, 30L))
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
  out <- tbl_in %>%
    presto_unnest(
      arr,
      values_to = "val",
      with_ordinality = TRUE,
      ordinality_to = "idx"
    ) %>%
    dplyr::collect()

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

  out <- subq %>%
    presto_unnest(arr, values_to = "elem") %>%
    dplyr::collect()

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

  out <- cte_rel %>%
    presto_unnest(arr2, values_to = "elem") %>%
    dplyr::collect()

  expect_equal(names(out), c("id", "arr2", "elem"))
  expect_equal(sort(out$elem), c(1L, 2L, 3L, 4L, 5L))
})


