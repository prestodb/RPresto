# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbClearResult')

source('utilities.R')

test_that('dbClearResult works with live database', {
  conn <- setup_live_connection()
  result <- dbSendQuery(conn, 'SELECT 1')
  expect_true(dbClearResult(result))
})

test_that('dbClearResult works with mock', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        url='http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT 1',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        url='http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT 2',
        next_uri='http://localhost:8000/query_2/1'
      ),
      mock_httr_response(
        url='http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT 3'
      )
    ),
    `httr::DELETE`=mock_httr_replies(
      mock_httr_response(
        url='http://localhost:8000/query_1/1',
        status_code=200,
        state=''
      ),
      mock_httr_response(
        url='http://localhost:8000/query_2/1',
        status_code=500,
        state=''
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT 1')
      expect_true(dbClearResult(result), label='regular query')
      expect_true(dbClearResult(result), label='idempotency')

      result <- dbSendQuery(conn, 'SELECT 2')
      expect_false(dbClearResult(result), label='DELETE fails')

      result <- dbSendQuery(conn, 'SELECT 3')
      expect_true(dbClearResult(result), label='complete query')
    }
  )
})
