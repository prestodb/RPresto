# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("db_explain")

test_that("db_explain works with live database", {
  s <- setup_live_dplyr_connection()[["db"]]

  explanation <- dplyr::db_explain(s[["con"]], dplyr::sql("SHOW TABLES"))
  expect_is(explanation[[1]], "character")

  expect_error(
    dplyr::db_explain(s[["con"]], dplyr::sql("INVALID")),
    "Query.*failed:.*(no viable alternative at|mismatched) input 'INVALID'"
  )
})

test_that("db_explain works with mock", {
  s <- setup_mock_dplyr_connection()[["db"]]
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "QUEUED",
        request_body = "EXPLAIN SHOW TABLES",
        next_uri = "http://localhost:8000/query_1/1"
      )
    ),
    `httr::GET` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/query_1/1",
        status_code = 200,
        data = data.frame(e = "explanation", stringsAsFactors = FALSE),
        state = "FINISHED"
      )
    ),
    {
      result <- dplyr::db_explain(s[["con"]], dplyr::sql("SHOW TABLES"))
      expect_equal(result, "explanation")
    }
  )
})
