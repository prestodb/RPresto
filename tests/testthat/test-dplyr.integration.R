# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dplyr")

test_that("dplyr integration works", {
  parts <- setup_live_dplyr_connection()
  db <- parts[["db"]]
  tablename <- parts[["iris_table_name"]]

  expect_that(db, is_a("src_sql"))
  iris_presto <- dplyr::tbl(db, tablename)
  expect_that(iris_presto, is_a("tbl_presto"))
  expect_that(ncol(iris_presto), equals(5))

  # collect() works
  expect_that(
    nrow(dplyr::collect(iris_presto, n = Inf)),
    equals(nrow(iris_df))
  )

  iris_presto_summary <- dplyr::arrange(
    dplyr::rename(
      dplyr::summarise(
        dplyr::group_by(iris_presto, species),
        mean_sepal.length = mean(as(sepal.length, 0.0), na.rm = TRUE)
      ),
      Species = species
    ),
    Species
  )
  iris_summary <- tibble::as_tibble(
    dplyr::arrange(
      dplyr::summarise(
        dplyr::group_by(
          dplyr::mutate(
            iris,
            Species = as.character(Species)
          ),
          Species
        ),
        mean_sepal.length = mean(Sepal.Length, na.rm = TRUE)
      ),
      Species
    )
  )
  expect_equal_data_frame(dplyr::collect(iris_presto_summary), iris_summary)

  # bigint handling can be specified in collect()
  iris_presto_bigint <- iris_presto %>%
    dplyr::mutate(bigint = sql("cast('9007199254740991' as bigint)")) %>%
    head(1) %>%
    dplyr::select(bigint)
  expect_warning(
    dplyr::collect(iris_presto_bigint),
    "NAs produced by integer overflow"
  )
  expect_equal_data_frame(
    dplyr::collect(iris_presto_bigint, bigint = "character"),
    tibble::tibble(bigint = "9007199254740991")
  )

  # collapse forces the tbl to be a subquery, therefore tests the
  # db_query_fields path that has `sql` as a subselect as opposed
  # to a table name. There is some behavioral change between dplyr
  # 0.4.3 vs 0.5.0 that no longer wraps the former in parentheses
  # so it should be tested.
  expect_that(
    nrow(dplyr::collect(dplyr::collapse(iris_presto), n = Inf)),
    equals(nrow(iris_df))
  )

  # compute() collapses a lazy query into a table
  iris_presto_summary_saved <-
    dplyr::compute(iris_presto_summary, "iris_presto_summary")
  expect_is(iris_presto_summary_saved, "tbl_presto")
  expect_true(dplyr::db_has_table(db$con, "iris_presto_summary"))
  expect_true(DBI::dbRemoveTable(db$con, "iris_presto_summary"))

  # compute() works with CTE
  iris_presto_summary_cte <- dplyr::compute(
    iris_presto_summary, "iris_presto_summary", cte = TRUE
  )
  iris_presto_summary_cte_saved <- dplyr::compute(
    iris_presto_summary_cte, "iris_presto_summary_cte"
  )
  expect_is(iris_presto_summary_cte_saved, "tbl_presto")
  expect_true(dplyr::db_has_table(db$con, "iris_presto_summary_cte"))
  expect_true(DBI::dbRemoveTable(db$con, "iris_presto_summary_cte"))
})
