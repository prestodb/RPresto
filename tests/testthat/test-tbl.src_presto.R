# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("tbl.src_presto")

source("utilities.R")

test_that("dplyr::tbl works", {
  parts <- setup_live_dplyr_connection()

  iris_presto <- dplyr::tbl(parts[["db"]], parts[["iris_table_name"]])
  iris_presto_materialized <- dplyr::collect(iris_presto, n = Inf)
  expect_that(
    nrow(iris_presto_materialized),
    equals(nrow(iris))
  )

  iris_from_sql <- dplyr::tbl(
    parts[["db"]],
    dplyr::sql(paste("SELECT * FROM", parts[["iris_table_name"]]))
  )
  expect_equal(
    dplyr::arrange(
      iris_presto_materialized,
      sepal_width,
      sepal_length,
      petal_width,
      petal_length,
      species
    ),
    dplyr::arrange(
      dplyr::collect(iris_from_sql, n = Inf),
      sepal_width,
      sepal_length,
      petal_width,
      petal_length,
      species
    ),
  )
})

test_that("bigint parameter works", {
  parts <- setup_live_dplyr_connection(bigint = "integer64")
  iris_presto <- dplyr::tbl(parts[["db"]], parts[["iris_table_name"]])
  iris_presto_with_bigint <- dplyr::mutate(
    iris_presto,
    bigint = sql("cast(1152921504606846976 as bigint)")
  )
  iris_presto_materialized <- dplyr::collect(iris_presto_with_bigint, n = Inf)
  expect_s3_class(iris_presto_materialized$bigint, "integer64")

  parts <- setup_live_dplyr_connection(bigint = "character")
  iris_presto <- dplyr::tbl(parts[["db"]], parts[["iris_table_name"]])
  iris_presto_with_bigint <- dplyr::mutate(
    iris_presto,
    bigint = sql("cast(1152921504606846976 as bigint)")
  )
  iris_presto_materialized <- dplyr::collect(iris_presto_with_bigint, n = Inf)
  expect_type(iris_presto_materialized$bigint, "character")

  parts <- setup_live_dplyr_connection(bigint = "numeric")
  iris_presto <- dplyr::tbl(parts[["db"]], parts[["iris_table_name"]])
  iris_presto_with_bigint <- dplyr::mutate(
    iris_presto,
    bigint = sql("cast(1152921504606846976 as bigint)")
  )
  expect_warning(
    iris_presto_materialized <- dplyr::collect(iris_presto_with_bigint, n = Inf),
    "integer precision lost while converting to double"
  )
  expect_type(iris_presto_materialized$bigint, "double")
})
