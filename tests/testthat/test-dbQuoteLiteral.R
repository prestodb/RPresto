# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbQuoteLiteral")

source("utilities.R")

test_that("dbQuoteLiteral works with live connection", {
  conn <- presto_default()
  expect_equal(dbQuoteLiteral(conn, DBI::SQL("foo")), DBI::SQL("foo"))
  expect_equal(
    dbQuoteLiteral(conn, factor(c("a", "b", NA_character_))),
    DBI::SQL(c("'a'", "'b'", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(conn, c("a", "b", NA_character_)),
    DBI::SQL(c("'a'", "'b'", "NULL"))
  )
  ts1 <- lubridate::ymd_hms("2000-01-01 01:02:03", tz = "America/Los_Angeles")
  ts2 <- lubridate::ymd_hms("2000-01-02 04:05:06", tz = "America/Los_Angeles")
  expect_equal(
    dbQuoteLiteral(
      conn,
      c(
        ts1,
        ts2,
        as.POSIXct(NA)
      )
    ),
    DBI::SQL(
      c(
        paste0(
          "TIMESTAMP '",
          strftime(ts1, "%Y-%m-%d %H:%M:%S", tz = conn@session.timezone),
          "'"
        ),
        paste0(
          "TIMESTAMP '",
          strftime(ts2, "%Y-%m-%d %H:%M:%S", tz = conn@session.timezone),
          "'"
        ),
        "NULL"
      )
    )
  )
  expect_equal(
    dbQuoteLiteral(conn, as.Date(c("2000-01-01", "2000-01-02", NA_character_))),
    DBI::SQL(c("DATE '2000-01-01'", "DATE '2000-01-02'", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(conn, hms::as_hms(c("01:02:03", "04:05:06", NA_character_))),
    DBI::SQL(c("TIME '01:02:03'", "TIME '04:05:06'", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(conn, c(TRUE, FALSE, NA)),
    DBI::SQL(c("true", "false", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(conn, list(charToRaw("abc"), charToRaw("def"), charToRaw(NA_character_))),
    DBI::SQL(c("X'616263'", "X'646566'", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(conn, c(1L, 2L, NA_integer_)),
    DBI::SQL(c("1", "2", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(conn, c(1.1, 2.2, NA_real_)),
    DBI::SQL(
      c("CAST(1.1 AS DOUBLE)", "CAST(2.2 AS DOUBLE)", "CAST(NULL AS DOUBLE)")
    )
  )
  expect_equal(
    dbQuoteLiteral(conn, list(c(1L, 2L), c(3L, NA_integer_), NA_integer_)),
    DBI::SQL(c("ARRAY[1, 2]", "ARRAY[3, NULL]", "NULL"))
  )
  expect_equal(
    dbQuoteLiteral(
      conn,
      list(c("a", "b"), c("c", NA_character_), NA_character_)
    ),
    DBI::SQL(c("ARRAY['a', 'b']", "ARRAY['c', NULL]", "NULL"))
  )
  expect_error(
    dbQuoteLiteral(
      conn,
      list(c("a" = 1L, "b" = 2L), c("a" = 3L, "b" = NA_integer_), NA_integer_)
    ),
    "MAP values are not supported."
  )
})
