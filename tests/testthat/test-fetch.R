# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('fetch')

source('utilities.R')

test_that('fetch works with live database', {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, 'SELECT 1 AS n')
  expect_error(
    fetch(result, 1),
    '.*fetching custom number of rows.*is not supported.*'
  )
  expect_equal(fetch(result, -1), data.frame(n=1))
  expect_true(dbHasCompleted(result))

  result <- dbSendQuery(conn, 'SELECT 2 AS n')
  df <- fetch(result)
  expect_true(dbIsValid(result))
})

test_that('fetch works with mock', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 1 AS n',
        next_uri='http://localhost:8000/query_1/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(n=1),
        state='FINISHED',
        next_uri='http://localhost:8000/query_1/2'
      ),
      mock_httr_response(
        'http://localhost:8000/query_1/2',
        status_code=200,
        data=data.frame(n=2),
        state='FINISHED'
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT 1 AS n')
      expect_error(
        fetch(result, 1),
        '.*fetching custom number of rows.*is not supported.*'
      )
      expect_equal(fetch(result), data.frame(n=1))
      expect_equal(fetch(result, -1), data.frame(n=2))
      expect_true(dbHasCompleted(result))

      result <- dbSendQuery(conn, 'SELECT 1 AS n')
      expect_equal(fetch(result, -1), data.frame(n=c(1, 2)))
    }
  )
})

test_that('fetch works with duplicate column names', {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, 'SELECT 0 AS dummy, 1 AS dummy')
  expect_equal_data_frame(
    dbFetch(result, n=Inf),
    data.frame(dummy=0, dummy=1, check.names=FALSE)
  )
})
