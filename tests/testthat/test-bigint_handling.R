# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "bigint handling"))

test_that("Non 32-bit integers give warning", {
  conn <- setup_live_connection()

  expect_warning(
    dbGetQuery(conn, "SELECT CAST('2147483648' AS BIGINT)"),
    "NAs produced by integer overflow"
  )
  expect_warning(
    dbGetQuery(conn, "SELECT CAST('-2147483649' AS BIGINT)"),
    "NAs produced by integer overflow"
  )
})

test_that("BIGINT within [-9007199254740991, 9007199254740991] range works", {
  conn <- setup_live_connection()

  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "SELECT CAST('9007199254740991' AS BIGINT) AS integer64",
      bigint = "integer64"
    ),
    tibble::tibble(integer64 = bit64::as.integer64("9007199254740991"))
  )
  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "SELECT CAST('9007199254740991' AS BIGINT) AS character",
      bigint = "character"
    ),
    tibble::tibble(character = "9007199254740991")
  )
  expect_equal_data_frame(
    db.bigint.numeric <- dbGetQuery(
      conn,
      "SELECT CAST('9007199254740991' AS BIGINT) AS numeric",
      bigint = "numeric"
    ),
    tibble::tibble(numeric = as.numeric("9007199254740991"))
  )
})

test_that("BIGINT greater than 9007199254740991 gives warning", {
  conn <- setup_live_connection()

  expect_warning(
    dbGetQuery(
      conn,
      "SELECT CAST('9007199254740992' AS BIGINT)",
      bigint = "numeric"
    ),
    "integer precision lost while converting to double"
  )
  expect_warning(
    dbGetQuery(
      conn,
      "SELECT CAST('-9007199254740992' AS BIGINT)",
      bigint = "numeric"
    ),
    "integer precision lost while converting to double"
  )
})
