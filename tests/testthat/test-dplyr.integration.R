# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dplyr')

source('utilities.R')

test_that('dplyr integration works', {
  if(!require('dplyr', quietly=TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }

  parts <- setup_dplyr_connection()
  db <- parts[['db']]
  tablename <- parts[['iris_table_name']]

  expect_that(db, is_a("src_sql"))

  iris_presto <- dplyr::tbl(db, tablename)

  expect_that(iris_presto %>% head(5) %>% nrow, equals(5))
  expect_that(ncol(iris_presto), equals(5))

  expect_that(iris_presto, is_a('tbl_presto'))

  iris_presto_summary <- iris_presto %>%
    group_by(species) %>%
    summarise(mean_sepal_length = mean(sepal_length)) %>%
    arrange(species) %>%
    rename(Species = species) %>%
    collect %>%
    as.data.frame

  iris_summary <- iris %>%
    mutate(Species = as.character(Species)) %>%
    group_by(Species) %>%
    summarise(mean_sepal_length = mean(Sepal.Length)) %>%
    arrange(Species) %>%
    as.data.frame

  expect_equal_data_frame(iris_presto_summary, iris_summary)
})
