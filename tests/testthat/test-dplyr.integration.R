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


  expect_that(
    nrow(as.data.frame(iris_presto, n=Inf)),
    equals(nrow(iris))
  )
  expect_that(ncol(iris_presto), equals(5))

  expect_that(iris_presto, is_a('tbl_presto'))

  # collapse forces the tbl to be a subquery, therefore tests the
  # db_query_fields path that has `sql` as a subselect as opposed
  # to a table name. There is some behavioral change between dplyr
  # 0.4.3 vs 0.5.0 that no longer wraps the former in parentheses
  # so it should be tested.
  expect_that(
    nrow(as.data.frame(dplyr::collapse(iris_presto), n=Inf)),
    equals(nrow(iris))
  )

  iris_presto_summary <- as.data.frame(dplyr::collect(
    dplyr::rename(
      dplyr::arrange(
        dplyr::summarise(
          dplyr::group_by(iris_presto, species),
          mean_sepal_length = mean(as(sepal_length, 0.0), na.rm=TRUE)
        ),
        species
      ),
      Species=species
    ),
    n=Inf
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
        mean_sepal_length=mean(Sepal.Length, na.rm=TRUE)
      ),
      Species
    )
  )

  expect_equal_data_frame(iris_presto_summary, iris_summary)
})
