# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('integration')

source('utilities.R')

test_that("Connections handle port argument correctly", {
  test_port <- function(port) {
    dbConnect(RPresto::Presto(),
      schema='test_schema',
      catalog='test_catalog',
      host='localhost',
      port=port,
      user=Sys.getenv('USER')
    )
  }

  expect_that(test_port(NULL),
              throws_error("Please specify a port as an integer"))
  expect_that(test_port('NOT A NUMBER'),
              throws_error("Please specify a port as an integer"))
})

test_that('Integration tests work', {
  conn <- setup_live_connection()

  expect_that(conn, is_a("PrestoConnection"))

  sql <- paste('SELECT * FROM', iris.sql(), 'LIMIT 5')

  rs <- dbSendQuery(conn, sql)
  expect_that(rs, is_a("PrestoResult"))

  df <- NULL
  while (!dbHasCompleted(rs)) {
    chunk <- dbFetch(rs)
    if (NROW(chunk)) {
      df <- if (is.null(df)) {
          chunk
        } else {
          rbind(df, chunk)
        }
    }
  }
  expect_that(df, is_a("data.frame"))
  expect_that(nrow(df), equals(5))
  expect_that(ncol(df), equals(5))

  expect_that(dbClearResult(rs), is_true())

  df <- dbGetQuery(conn, sql)
  expect_that(df, is_a("data.frame"))
  expect_that(nrow(df), equals(5))
  expect_that(ncol(df), equals(5))

  tbls <- dbListTables(conn)
  expect_that(tbls, is_a("character"))
  expect_that(length(tbls), is_more_than(0))

  expect_that(dbDisconnect(conn), is_true())
})
