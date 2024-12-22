# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbListTables and db_list_tables"))

test_that("dbListTables works with live database", {
  conn <- setup_live_connection()

  expect_gt(length(dbListTables(conn)), 0)
  expect_gt(length(db_list_tables(conn)), 0)
  expect_equal(
    dbListTables(conn, pattern = "__non_existent_table__"),
    character(0)
  )
})

test_that("dbListTables works with mock", {
  conn <- setup_mock_connection()

  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "QUEUED",
        request_body = "^SHOW TABLES$",
        next_uri = "http://localhost:8000/query_1/1"
      ),
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "QUEUED",
        request_body = "^SHOW TABLES LIKE '_no_table_'$",
        next_uri = "http://localhost:8000/query_2/1"
      )
    ),
    `httr::GET` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/query_1/1",
        status_code = 200,
        data = data.frame(Table = letters, stringsAsFactors = FALSE),
        state = "FINISHED"
      ),
      mock_httr_response(
        "http://localhost:8000/query_2/1",
        status_code = 200,
        data = data.frame(Table = "a", stringsAsFactors = FALSE)[FALSE, ],
        state = "FINISHED"
      )
    ),
    {
      expect_equal(dbListTables(conn), letters)
      expect_equal(dbListTables(conn, pattern = "_no_table_"), character(0))
    }
  )
})
