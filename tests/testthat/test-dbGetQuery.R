# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbGetQuery")

source("utilities.R")

with_locale(test.locale(), test_that)("dbGetQuery works with live database", {
  conn <- setup_live_connection()
  expect_equal_data_frame(
    dbGetQuery(conn, "SELECT n FROM (VALUES (1), (2)) AS t (n)"),
    tibble::tibble(n = c(1, 2))
  )

  # Unicode expression of c('çğıöşü', 'ÇĞİÖŞÜ')
  # Written explicitly to ensure we have a utf-8 vector
  utf8.tr.characters <- c(
    "\u00E7\u011F\u0131\u00F6\u015F\u00FC",
    "\u00C7\u011E\u0130\u00D6\u015E\u00DC"
  )
  # The characters above in iso8859-9 - the test locale
  lowercase <- "\xE7\xF0\xFD\xF6\xFE\xFC"
  uppercase <- "\xC7\xD0\xDD\xD6\xDE\xDC"
  query <- sprintf(
    "SELECT t FROM (VALUES ('%s'), ('%s')) AS a (t)",
    lowercase,
    uppercase
  )
  expect_equal(Encoding(query), "unknown")

  r <- dbGetQuery(conn, query)
  expect_equal(Encoding(r[["t"]]), c("UTF-8", "UTF-8"))

  e <- tibble::tibble(t = utf8.tr.characters)
  expect_equal_data_frame(r, e)
})

with_locale(test.locale(), test_that)("dbGetQuery works with mock", {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "QUEUED",
        request_body = "SELECT n FROM two_rows",
        next_uri = "http://localhost:8000/query_1/1"
      ),
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "QUEUED",
        request_body = "SELECT t FROM encoding_test",
        next_uri = "http://localhost:8000/query_2/1"
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
      ),
      mock_httr_response(
        "http://localhost:8000/query_2/1",
        status_code = 200,
        data = data.frame(t = "çğıöşü", stringsAsFactors = FALSE),
        state = "FINISHED",
        next_uri = "http://localhost:8000/query_2/2"
      ),
      mock_httr_response(
        "http://localhost:8000/query_2/2",
        status_code = 200,
        data = data.frame(t = "ÇĞİÖŞÜ"),
        state = "FINISHED"
      )
    ),
    {
      expect_equal_data_frame(
        dbGetQuery(conn, "SELECT n FROM two_rows"),
        tibble::tibble(n = c(1, 2))
      )
      expect_equal_data_frame(
        dbGetQuery(conn, "SELECT t FROM encoding_test"),
        tibble::tibble(t = c("çğıöşü", "ÇĞİÖŞÜ"))
      )
    }
  )
})

test_that("dbGetQuery works with data in POST response", {
  conn <- setup_mock_connection()

  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SELECT 1 AS x",
        data = data.frame(x = 1, stringsAsFactors = FALSE)
      ),
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SELECT n FROM two_rows",
        data = data.frame(n = 3, stringsAsFactors = FALSE),
        next_uri = "http://localhost:8000/query_1/1"
      )
    ),
    `httr::GET` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/query_1/1",
        status_code = 200,
        data = data.frame(n = 4, stringsAsFactors = FALSE),
        state = "FINISHED"
      )
    ),
    {
      expect_equal_data_frame(
        dbGetQuery(conn, "SELECT 1 AS x"),
        tibble::tibble(x = 1)
      )

      expect_equal_data_frame(
        dbGetQuery(conn, "SELECT n FROM two_rows"),
        tibble::tibble(n = c(3, 4))
      )
    }
  )
})

test_that("Inconsistent data in chunks fail", {
  conn <- setup_mock_connection()

  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "PLANNING",
        request_body = "SELECT a, b FROM broken_chunks",
        next_uri = "http://localhost:8000/query_1/1",
        query_id = "query_1"
      )
    ),
    `httr::GET` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/query_1/1",
        status_code = 200,
        state = "PLANNING",
        next_uri = "http://localhost:8000/query_1/2"
      ),
      mock_httr_response(
        "http://localhost:8000/query_1/2",
        status_code = 200,
        state = "RUNNING",
        data = data.frame(a = 1, b = TRUE),
        next_uri = "http://localhost:8000/query_1/3"
      ),
      mock_httr_response(
        "http://localhost:8000/query_1/3",
        status_code = 200,
        state = "RUNNING",
        data = data.frame(x = 1, y = TRUE),
        next_uri = "http://localhost:8000/query_1/4"
      ),
      mock_httr_response(
        "http://localhost:8000/query_1/4",
        status_code = 200,
        state = "FINISHED",
        data = data.frame(a = 3, b = FALSE)
      )
    ),
    `httr::DELETE` = mock_httr_replies(
      mock_httr_response(
        url = "http://localhost:8000/v1/query/query_1",
        status_code = 200,
        state = ""
      )
    ),
    {
      expect_error(
        dbGetQuery(conn, "SELECT a, b FROM broken_chunks"),
        "Chunk column names are different across chunks"
      )
    }
  )
})
