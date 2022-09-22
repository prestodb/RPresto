# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("dbConnect")

test_that("dbConnect constructs PrestoConnection correctly", {
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
      expect_error(dbConnect(RPresto::Presto()), label = "not enough arguments")

      expect_error(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx"
        ),
        'argument ".*" is missing, with no default',
        label = "not enough arguments"
      )

      expect_error(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx",
          schema = "test",
          host = "http://localhost",
          port = "",
          user = Sys.getenv("USER")
        ),
        "Please specify a port as an integer",
        label = "invalid port"
      )

      expect_is(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx",
          schema = "test",
          host = "http://localhost",
          port = 8000,
          source = "testsource",
          session.timezone = test.timezone(),
          user = Sys.getenv("USER")
        ),
        "PrestoConnection"
      )

      expect_is(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx",
          schema = "test",
          host = "http://localhost",
          port = 8000,
          source = "testsource",
          session.timezone = test.timezone(),
          user = Sys.getenv("USER"),
          parameters = list(
            experimental_big_query = "true"
          )
        ),
        "PrestoConnection",
        label = "extra parameters"
      )

      expect_equal(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx",
          schema = "test",
          host = "http://localhost",
          port = 8000,
          source = "testsource",
          session.timezone = test.timezone(),
          user = Sys.getenv("USER"),
          bigint = "character",
          parameters = list(
            experimental_big_query = "true"
          )
        )@bigint,
        "character"
      )

      expect_true(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx",
          schema = "test",
          host = "http://localhost",
          port = 8000,
          source = "testsource",
          session.timezone = test.timezone(),
          user = Sys.getenv("USER"),
          bigint = "character",
          request.config = httr::verbose()
        )@request.config$options$verbose
      )

      expect_equal(
        length(
          dbConnect(
            RPresto::Presto(),
            catalog = "jmx",
            schema = "test",
            host = "http://localhost",
            port = 8000,
            source = "testsource",
            session.timezone = test.timezone(),
            user = Sys.getenv("USER"),
            ctes = list("test_cte" = "SELECT 1")
          )@session$.ctes
        ),
        1
      )

      expect_error(
        dbConnect(
          RPresto::Presto(),
          catalog = "jmx",
          schema = "test",
          host = "http://localhost",
          port = 8000,
          source = "testsource",
          session.timezone = test.timezone(),
          user = Sys.getenv("USER"),
          bigint = "wrongarg",
          parameters = list(
            experimental_big_query = "true"
          )
        ),
        "should be one of"
      )
    }
  )
})
