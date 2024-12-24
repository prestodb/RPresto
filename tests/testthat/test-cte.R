# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "ctes"))

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
  iris_presto_mean <- iris_presto |>
    dplyr::group_by(species) |>
    dplyr::summarize(
      across(.fns = ~mean(., na.rm = TRUE), .names = "mean_{.col}"),
      .groups = "drop"
    )
  expect_is(iris_presto_mean$lazy_query, "lazy_select_query")
  # One-level CTE works
  iris_presto_mean_cte <- iris_presto_mean |>
    dplyr::compute("iris_species_mean", cte = TRUE)
  expect_true(db$con@session$hasCTE("iris_species_mean"))
  expect_is(iris_presto_mean_cte$lazy_query, "lazy_base_remote_query")
  expect_equal_data_frame(
    dplyr::arrange(dplyr::collect(iris_presto_mean), species),
    dplyr::arrange(dplyr::collect(iris_presto_mean_cte), species)
  )
  expect_true(RPresto:::is_cte_used(dbplyr::remote_query(iris_presto_mean_cte)))
  # Nested CTEs work
  iris_presto_mean_2 <- iris_presto_mean_cte |>
    dplyr::summarize(
      across(-species, ~mean(., na.rm = TRUE))
    )
  expect_is(iris_presto_mean_2$lazy_query, "lazy_select_query")
  iris_presto_mean_2_cte <- iris_presto_mean_2 |>
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
      mean_sepal.length = mean(sepal.length, na.rm = TRUE)
    ),
    name = "iris_width", cte = TRUE
  )
  expect_equal(db$con@session$getCTENames(), c("iris_width"))
  iris_presto.length <- dplyr::compute(
    dplyr::summarize(
      dplyr::group_by(iris_presto, species),
      mean_sepal.width = mean(sepal.width, na.rm = TRUE)
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
    dplyr::arrange(dplyr::collect(iris_presto.join), species),
    dplyr::arrange(dplyr::collect(iris_presto.join_cte), species)
  )
})

test_that("CTEs using joins work", {
  parts <- setup_live_dplyr_connection()
  db <- parts[["db"]]
  tablename <- parts[["iris_table_name"]]
  iris_presto <- dplyr::tbl(db, tablename)
  iris_presto.avg <- dplyr::compute(
    dplyr::summarize(
      dplyr::group_by(iris_presto, species),
      dplyr::across(
        sepal.length:petal.width,
        ~ mean(., na.rm = TRUE),
        .names = "{.col}_avg"
      )
    ),
    name = "iris_avg",
    cte = TRUE
  )
  iris.avg <- dplyr::summarize(
    dplyr::group_by(iris, Species),
    dplyr::across(
      Sepal.Length:Petal.Width,
      ~ mean(., na.rm = TRUE),
      .names = "{.col}_avg"
    )
  )
  expect_equal(dplyr::pull(dplyr::tally(iris_presto.avg), n), nrow(iris.avg))
  iris_presto.inner_join <- dplyr::compute(
    dplyr::inner_join(
      iris_presto,
      iris_presto.avg,
      by = "species"
    ),
    name = "iris_inner_join",
    cte = TRUE
  )
  iris.inner_join <- dplyr::inner_join(
    iris,
    iris.avg,
    by = "Species"
  )
  expect_equal(
    dplyr::pull(dplyr::tally(iris.inner_join), n), nrow(iris.inner_join)
  )
  iris_presto.left_join <- dplyr::compute(
    dplyr::left_join(
      iris_presto,
      iris_presto.avg,
      by = "species"
    ),
    name = "iris_left_join",
    cte = TRUE
  )
  iris.left_join <- dplyr::left_join(
    iris,
    iris.avg,
    by = "Species"
  )
  expect_equal(
    dplyr::pull(dplyr::tally(iris.left_join), n), nrow(iris.left_join)
  )
  iris_presto.right_join <- dplyr::compute(
    dplyr::right_join(
      iris_presto,
      dplyr::filter(iris_presto.avg, species == "virginica"),
      by = "species"
    ),
    name = "iris_right_join",
    cte = TRUE
  )
  iris.right_join <- dplyr::right_join(
    iris,
    iris.avg,
    by = "Species"
  )
  expect_equal(
    dplyr::pull(dplyr::tally(iris.right_join), n), nrow(iris.right_join)
  )
  iris_presto.full_join <- dplyr::compute(
    dplyr::full_join(
      iris_presto,
      dplyr::filter(iris_presto.avg, species == "virginica"),
      by = "species"
    ),
    name = "iris_full_join",
    cte = TRUE
  )
  iris.full_join <- dplyr::full_join(
    iris,
    iris.avg,
    by = "Species"
  )
  expect_equal(
    dplyr::pull(dplyr::tally(iris.full_join), n), nrow(iris.full_join)
  )
})

test_that("CTEs using union work", {
  parts <- setup_live_dplyr_connection()
  db <- parts[["db"]]
  tablename <- parts[["iris_table_name"]]
  iris_presto <- dplyr::tbl(db, tablename)
  iris_presto.virginica <- dplyr::compute(
    dplyr::summarize(
      dplyr::group_by(
        dplyr::filter(
          iris_presto,
          species == "virginica"
        ),
        species
      ),
      mean_sepal.length = mean(sepal.length, na.rm = TRUE)
    ),
    name = "iris_width_virginica", cte = TRUE
  )
  expect_equal(db$con@session$getCTENames(), c("iris_width_virginica"))
  iris_presto.setosa <- dplyr::compute(
    dplyr::summarize(
      dplyr::group_by(
        dplyr::filter(
          iris_presto,
          species == "setosa"
        ),
        species
      ),
      mean_sepal.length = mean(sepal.length, na.rm = TRUE)
    ),
    name = "iris_width_setosa", cte = TRUE
  )
  expect_equal(
    db$con@session$getCTENames(), c("iris_width_virginica", "iris_width_setosa")
  )
  iris_presto.union <- dplyr::compute(
    dplyr::union_all(
      iris_presto.virginica,
      iris_presto.setosa
    ),
    name = "iris_presto_union", cte = TRUE
  )
  expect_equal(
    db$con@session$getCTENames(),
    c("iris_width_virginica", "iris_width_setosa", "iris_presto_union")
  )
})
