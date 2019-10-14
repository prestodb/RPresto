# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('tbl.src_presto')

source('utilities.R')

test_that('dplyr::tbl works', {
  parts <- setup_live_dplyr_connection()

  iris_presto <- dplyr::tbl(parts[['db']], parts[['iris_table_name']])
  iris_presto_materialized <- as.data.frame(iris_presto, n=Inf)
  expect_that(
    nrow(iris_presto_materialized),
    equals(nrow(iris))
  )

  iris_from_sql <- dplyr::tbl(
    parts[['db']],
    dplyr::sql(paste('SELECT * FROM', parts[['iris_table_name']]))
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
      as.data.frame(dplyr::collect(iris_from_sql, n=Inf)),
      sepal_width,
      sepal_length,
      petal_width,
      petal_length,
      species
    ),
  )
})
