# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

setup_mock_connection <- function() {
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SELECT current_timezone() AS tz",
        data = data.frame(tz = Sys.timezone(), stringsAsFactors = FALSE)
      )
    ),
    {
      mock.conn <- dbConnect(
        RPresto::Presto(),
        schema = "test",
        catalog = "catalog",
        host = "http://localhost",
        port = 8000,
        source = "RPresto Test",
        session.timezone = test.timezone(),
        user = Sys.getenv("USER")
      )
      return(mock.conn)
    }
  )
}

setup_mock_dplyr_connection <- function() {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SELECT current_timezone() AS tz",
        data = data.frame(tz = Sys.timezone(), stringsAsFactors = FALSE)
      )
    ),
    {
      db <- src_presto(
        schema = "test",
        catalog = "catalog",
        host = "http://localhost",
        port = 8000,
        source = "RPresto Test",
        user = Sys.getenv("USER"),
        session.timezone = test.timezone(),
        parameters = list()
      )

      return(list(db = db, iris_table_name = "iris_table"))
    }
  )
}
