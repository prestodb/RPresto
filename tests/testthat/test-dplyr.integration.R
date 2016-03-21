# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dplyr')

source('utilities.R')

test_that('dplyr integration works', {
  parts <- setup_live_dplyr_connection()
  db <- parts[['db']]
  tablename <- parts[['iris_table_name']]

  expect_that(db, is_a("src_sql"))

  iris_presto <- dplyr::tbl(db, tablename)

  expect_that(nrow(head(iris_presto, 5)), equals(5))
  expect_that(ncol(iris_presto), equals(5))

  expect_that(iris_presto, is_a('tbl_presto'))

  iris_presto_summary <- as.data.frame(dplyr::collect(
    dplyr::rename(
      dplyr::arrange(
        dplyr::summarise(
          dplyr::group_by(iris_presto, species),
          mean_sepal_length = mean(as(sepal_length, 0.0))
        ),
        Species
      ),
      Species=species
    )
  ))

  iris_summary <- as.data.frame(
    dplyr::arrange(
      dplyr::summarise(
        dplyr::group_by(
          dplyr::mutate(
            iris,
            Species=as.character(Species)
          ),
          Species
        ),
        mean_sepal_length=mean(Sepal.Length)
      ),
      Species
    )
  )

  expect_equal_data_frame(iris_presto_summary, iris_summary)
})
