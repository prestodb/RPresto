# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("PrestoQuery")

source("utilities.R")

test_that("PrestoQuery methods work correctly", {
  conn <- setup_mock_connection()
  post.response <- structure(
    list(
      url = "http://localhost:8000/v1/statement",
      status_code = 200,
      headers = list(
        "content-type" = "application/json"
      ),
      content = charToRaw(jsonlite::toJSON(
        list(
          stats = list(state = jsonlite::unbox("INITIAL")),
          id = jsonlite::unbox("http__localhost_8000_v1_statement"),
          infoUri = jsonlite::unbox("http://localhost:8000/v1/query/query_1")
        )
      ))
    ),
    class = "response"
  )
  with_mock(
    `httr::POST` = mock_httr_replies(
      list(
        url = "http://localhost:8000/v1/statement",
        response = post.response,
        request_body = "SELECT 1"
      )
    ),
    {
      query <- PrestoQuery(conn, "SELECT 1")
      res <- query$execute()
      expect_equal(query$id(), "http__localhost_8000_v1_statement")
      expect_equal(query$infoUri(), "http://localhost:8000/v1/query/query_1")
      expect_equal(query$nextUri(), "")
      expect_equal(query$state(), "INITIAL")
      expect_equal(query$fetchedRowCount(), 0)
      expect_equal(query$stats(), list(state = "INITIAL"))
      expect_true(query$hasCompleted())
      expect_equal(query$response(), post.response)
      expect_true(is.na(query$postDataFetched()))
      expect_true(query$postDataFetched(TRUE))
      expect_false(query$postDataFetched(FALSE))
      query$state("__TEST")
      expect_equal(query$state(), "__TEST")
    }
  )
})

test_that("PrestoQuery methods work correctly with POST data", {
  conn <- setup_mock_connection()
  mock_response <- mock_httr_response(
    url = "http://localhost:8000/v1/statement",
    status_code = 200,
    state = "FINISHED",
    data = data.frame(x = 1),
    request_body = "SELECT 1"
  )
  with_mock(
    `httr::POST` = mock_httr_replies(mock_response),
    {
      query <- PrestoQuery(conn, "SELECT 1")
      res <- query$execute()
      expect_equal(query$infoUri(), "")
      expect_equal(query$nextUri(), "")
      expect_equal(query$state(), "FINISHED")
      expect_equal(query$fetchedRowCount(), 0)
      expect_equal(query$stats(), list(state = "FINISHED"))
      expect_false(query$hasCompleted())
      expect_equal(query$response(), mock_response$response)
      expect_false(query$postDataFetched())
      expect_true(query$postDataFetched(TRUE))
      expect_true(query$postDataFetched())
      expect_true(query$hasCompleted())
    }
  )
})
