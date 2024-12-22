# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbGetRowCount"))

test_that("dbGetRowCount works with live database", {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, "SELECT 1 AS n")
  expect_equal(dbGetRowCount(result), 0)
  expect_equal(dbFetch(result, -1), tibble::tibble(n = 1))
  expect_equal(dbGetRowCount(result), 1)

  result <- dbSendQuery(conn, "SELECT n FROM (VALUES (1), (2)) AS t (n)")
  expect_equal(dbGetRowCount(result), 0)
  expect_equal(dbFetch(result, -1), tibble::tibble(n = c(1, 2)))
  expect_equal(dbGetRowCount(result), 2)
})

test_that("dbGetRowCount works with mock", {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "QUEUED",
        request_body = "SELECT n FROM two_rows",
        next_uri = "http://localhost:8000/query_1/1"
      )
    ),
    `httr::GET` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/query_1/1",
        status_code = 200,
        data = data.frame(n = 1, stringsAsFactors = FALSE),
        state = "FINISHED",
        next_uri = "http://localhost:8000/query_1/2"
      ),
      mock_httr_response(
        "http://localhost:8000/query_1/2",
        status_code = 200,
        data = data.frame(n = 2, stringsAsFactors = FALSE),
        state = "FINISHED"
      )
    ),
    {
      result <- dbSendQuery(conn, "SELECT n FROM two_rows")
      expect_equal(dbGetRowCount(result), 0)
      expect_equal(dbFetch(result), tibble::tibble(n = 1))
      expect_equal(dbGetRowCount(result), 1)
      expect_equal(dbFetch(result), tibble::tibble(n = 2))
      expect_equal(dbGetRowCount(result), 2)
      expect_equal(dbFetch(result), tibble::tibble())
      expect_equal(dbGetRowCount(result), 2)
    }
  )
})
