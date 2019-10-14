# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('bigint handling')

source('utilities.R')

test_that('Non 32-bit integers give warning', {
  conn <- setup_live_connection()

  expect_warning(
    dbGetQuery(conn, "SELECT 2147483648 AS a, 3 AS b"),
    'columns \\[1\\] are cast to double'
  )
  expect_warning(
    dbGetQuery(conn, "SELECT -3 AS d, -2147483649 AS c"),
    'columns \\[2\\] are cast to double'
  )
  rv <- suppressWarnings(dbGetQuery(conn, "SELECT 9223372036854775807 AS number"))
  expect_false(as.character(rv[['number']]) == "9223372036854775807")

  rv <- dbGetQuery(conn, "SELECT CAST(9223372036854775807 AS VARCHAR) AS string")
  expect_equal(rv[['string']], "9223372036854775807")
})
