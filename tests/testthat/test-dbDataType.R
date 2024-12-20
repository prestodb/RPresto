# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbDataType")

with_locale(test.locale(), test_that)("presto simple types are correct", {
  drv <- RPresto::Presto()

  expect_equal(dbDataType(drv, NULL), "VARCHAR")
  expect_equal(dbDataType(drv, TRUE), "BOOLEAN")
  expect_equal(dbDataType(drv, 1L), "INTEGER")
  expect_equal(dbDataType(drv, bit64::as.integer64(1L)), "BIGINT")
  expect_equal(dbDataType(drv, 1.0), "DOUBLE")
  expect_equal(dbDataType(drv, ""), "VARCHAR")
  expect_equal(dbDataType(drv, vector("raw", 0)), "VARBINARY")
  expect_equal(dbDataType(drv, as.Date("2015-03-01")), "DATE")
  expect_equal(dbDataType(drv, hms::as_hms("01:02:03")), "TIME")
  expect_equal(
    dbDataType(drv, as.POSIXct("2015-03-01 12:00:00", tz = "UTC")),
    "TIMESTAMP"
  )
  expect_equal(dbDataType(drv, factor()), "VARCHAR")
  expect_equal(dbDataType(drv, factor(ordered = TRUE)), "VARCHAR")
  expect_equal(
    dbDataType(drv, test_df),
    c(
      field1 = "VARCHAR",
      field2 = "INTEGER",
      field3 = "DOUBLE",
      field4 = "BOOLEAN",
      field5 = "DATE",
      field6 = "TIMESTAMP",
      field7 = "TIME",
      field8 = "ARRAY<INTEGER>",
      field9 = "ARRAY<DOUBLE>"
    )
  )
})

test_that("conversion to array is correct", {
  drv <- RPresto::Presto()

  expect_equal(
    dbDataType(
      drv,
      list(
        c(as.Date("2015-03-01"), as.Date("2015-03-02")),
        c(as.Date("2016-03-01"), as.Date("2016-03-02"))
      )
    ),
    "ARRAY<DATE>"
  )
  expect_equal(
    dbDataType(
      drv,
      list(
        list(
          c(as.Date("2015-03-01"), as.Date("2015-03-02")),
          c(as.Date("2016-03-01"), as.Date("2016-03-02"))
        ),
        list(
          c(as.Date("2015-04-01"), as.Date("2015-04-02")),
          c(as.Date("2016-04-01"), as.Date("2016-04-02"))
        )
      )
    ),
    "ARRAY<ARRAY<DATE>>"
  )
})

test_that("conversion to map is correct", {
  drv <- RPresto::Presto()

  expect_equal(
    dbDataType(
      drv,
      list(
        c("a" = 1L, "b" = 2L),
        c("a" = 3L, "b" = 4L)
      )
    ),
    "MAP<VARCHAR, INTEGER>"
  )
  expect_equal(
    dbDataType(
      drv,
      list(
        c("a" = as.Date("2015-03-01"), "b" = as.Date("2015-03-02")),
        c("a" = as.Date("2015-04-01"), "b" = as.Date("2015-04-02"))
      )
    ),
    "MAP<VARCHAR, DATE>"
  )
  expect_equal(
    dbDataType(
      drv,
      list(
        list(
          c("a" = as.Date("2015-03-01"), "b" = as.Date("2015-03-02")),
          c("a" = as.Date("2015-04-01"), "b" = as.Date("2015-04-02"))
        ),
        list(
          c("a" = as.Date("2016-03-01"), "b" = as.Date("2016-03-02")),
          c("a" = as.Date("2016-04-01"), "b" = as.Date("2016-04-02"))
        )
      )
    ),
    "ARRAY<MAP<VARCHAR, DATE>>"
  )
  expect_equal(
    dbDataType(
      drv,
      list(
        list(
          "aa" = c("a" = as.Date("2015-03-01"), "b" = as.Date("2015-03-02")),
          "ab" = c("a" = as.Date("2015-04-01"), "b" = as.Date("2015-04-02"))
        ),
        list(
          "aa" = c("a" = as.Date("2016-03-01"), "b" = as.Date("2016-03-02")),
          "ab" = c("a" = as.Date("2016-04-01"), "b" = as.Date("2016-04-02"))
        )
      )
    ),
    "MAP<VARCHAR, MAP<VARCHAR, DATE>>"
  )
})
