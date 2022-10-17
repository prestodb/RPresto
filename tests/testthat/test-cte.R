# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("ctes")

source("utilities.R")

test_that("CTEs created in dbConnect() works", {
  conn <- setup_live_connection(
    ctes = list(
      "dummy_values" =
        "SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c') ) AS t (id, name)"
    )
  )
  expect_equal_data_frame(
    dbReadTable(conn, "dummy_values"),
    tibble::tibble(
      id = c(1, 2, 3),
      name = c("a", "b", "c")
    )
  )
})

test_that("CTEs work in dplyr backend", {
  parts <- setup_live_dplyr_connection()
  db <- parts[["db"]]
  tablename <- parts[["iris_table_name"]]
  iris_presto <- dplyr::tbl(db, tablename)
  iris_presto_mean <- iris_presto %>%
    dplyr::group_by(species) %>%
    dplyr::summarize(
      across(.fns = mean, na.rm = TRUE, .names = "mean_{.col}"),
      .groups = "drop"
    ) %>%
    dplyr::arrange(species)
  expect_is(iris_presto_mean$lazy_query, "lazy_select_query")
  # One-level CTE works
  iris_presto_mean_cte <- iris_presto_mean %>%
    dplyr::compute("iris_species_mean", cte = TRUE)
  expect_true(db$con@session$hasCTE("iris_species_mean"))
  expect_is(iris_presto_mean_cte$lazy_query, "lazy_base_remote_query")
  expect_equal_data_frame(
    dplyr::collect(iris_presto_mean),
    dplyr::collect(iris_presto_mean_cte)
  )
  expect_true(RPresto:::is_cte_used(dbplyr::remote_query(iris_presto_mean_cte)))
  # Nested CTEs work
  iris_presto_mean_2 <- iris_presto_mean_cte %>%
    dplyr::summarize(
      across(-species, mean, na.rm = TRUE)
    )
  expect_is(iris_presto_mean_2$lazy_query, "lazy_select_query")
  iris_presto_mean_2_cte <- iris_presto_mean_2 %>%
    dplyr::compute("iris_mean", cte = TRUE)
  expect_true(db$con@session$hasCTE("iris_mean"))
  expect_is(iris_presto_mean_2_cte$lazy_query, "lazy_base_remote_query")
  expect_equal_data_frame(
    dplyr::collect(iris_presto_mean_2),
    dplyr::collect(iris_presto_mean_2_cte)
  )
  expect_true(
    RPresto:::is_cte_used(dbplyr::remote_query(iris_presto_mean_2_cte))
  )
})

test_that("Nested CTEs work", {
  parts <- setup_live_dplyr_connection()
  db <- parts[["db"]]
  tablename <- parts[["iris_table_name"]]
  iris_presto <- dplyr::tbl(db, tablename)
  iris_presto.width <- dplyr::compute(
    dplyr::summarize(
      dplyr::group_by(iris_presto, species),
      mean_sepal_length = mean(sepal_length, na.rm = TRUE)
    ),
    name = "iris_width", cte = TRUE
  )
  expect_equal(db$con@session$getCTENames(), c("iris_width"))
  iris_presto.length <- dplyr::compute(
    dplyr::summarize(
      dplyr::group_by(iris_presto, species),
      mean_sepal_width = mean(sepal_width, na.rm = TRUE)
    ),
    name = "iris_length", cte = TRUE
  )
  expect_equal(db$con@session$getCTENames(), c("iris_width", "iris_length"))
  iris_presto.join <- dplyr::arrange(
    dplyr::inner_join(
      iris_presto.width, iris_presto.length, by = "species"
    ),
    species
  )
  iris_presto.join_cte <- dplyr::compute(
    iris_presto.join,
    name = "iris_join", cte = TRUE
  )
  expect_equal(
    db$con@session$getCTENames(),
    c("iris_width", "iris_length", "iris_join")
  )
  expect_equal_data_frame(
    dplyr::collect(iris_presto.join),
    dplyr::collect(iris_presto.join_cte)
  )
})
