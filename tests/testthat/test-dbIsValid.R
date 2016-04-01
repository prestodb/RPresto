# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbIsValid')

source('utilities.R')

test_that('dbIsValid works with live database', {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, 'SELECT 1 AS n')
  expect_true(dbIsValid(result))
  expect_equal(dbFetch(result, -1), data.frame(n=1))
  expect_true(dbIsValid(result))

  result <- dbSendQuery(conn, 'SELECT 1 AS n')
  expect_true(dbIsValid(result))
  expect_true(dbClearResult(result))
  expect_false(dbIsValid(result))

  result <- dbSendQuery(conn, 'SELECT 1 FROM __nonexistent_table__')
  expect_true(dbIsValid(result))
  expect_error(dbFetch(result))
  expect_false(dbIsValid(result))

  expect_error(
    dbSendQuery(conn, 'INVALID SQL'),
    "Query.*failed:.*no viable alternative at input 'INVALID'"
  )
})

test_that('dbIsValid works with mock - successful queries', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT n FROM two_rows',
        next_uri='http://localhost:8000/query_1/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(n=1, stringsAsFactors=FALSE),
        state='FINISHED',
        next_uri='http://localhost:8000/query_1/2'
      ),
      mock_httr_response(
        'http://localhost:8000/query_1/2',
        status_code=200,
        data=data.frame(n=2, stringsAsFactors=FALSE),
        state='FINISHED'
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT n FROM two_rows')
      expect_true(dbIsValid(result))
      expect_equal(dbFetch(result), data.frame(n=1))
      expect_true(dbIsValid(result))
      expect_equal(dbFetch(result), data.frame(n=2))
      expect_true(dbIsValid(result))
      expect_equal(dbFetch(result), data.frame())
      expect_true(dbIsValid(result))
    }
  )
})

test_that('dbIsValid works with mock - retries and failures', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 1',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 2 AS n',
        next_uri='http://localhost:8000/query_2/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT "text" AS z',
        next_uri='http://localhost:8000/query_3/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT x FROM respond_with_404',
        next_uri='http://localhost:8000/query_4/1'
      )
    ),
    `httr::GET`=function(url, ...) {
      if (url == 'http://localhost:8000/query_1/1') {
        stop('Error')
      }
      if (url == 'http://localhost:8000/query_2/1') {
        if (request.count == 0) {
          request.count <<- 1
          stop('First request is an error')
        }
        return(mock_httr_replies(
          mock_httr_response(
            url,
            status_code=200,
            state='FINISHED',
            data=data.frame(n=2)
          )
        )(url))
      }
      if (url == 'http://localhost:8000/query_3/1') {
        if (request.count == 0) {
          request.count <<- 1
          return(mock_httr_replies(
            mock_httr_response(url, status_code=404)
          )(url))
        }
        return(mock_httr_replies(
          mock_httr_response(
            url,
            status_code=200,
            state='FINISHED',
            data=data.frame(z='text')
          )
        )(url))
      }
      if (url == 'http://localhost:8000/query_4/1') {
        return(mock_httr_replies(
          mock_httr_response(url, status_code=404)
        )(url))
      }
      stop('Unhandled url in httr::GET mock: ', url)
    },
    `httr::handle_reset`=function(...) return(),
    {
      result <- dbSendQuery(conn, 'SELECT 1')
      expect_true(dbIsValid(result))
      expect_error(
        suppressMessages(dbFetch(result)),
        'There was a problem with the request'
      )
      expect_false(dbIsValid(result))

      assign('request.count', 0, envir=environment(httr::GET))
      result <- dbSendQuery(conn, 'SELECT 2 AS n')
      expect_true(dbIsValid(result))
      expect_message(
        v <- dbFetch(result),
        'First request is an error'
      )
      expect_equal(v, data.frame(n=2))
      expect_true(dbIsValid(result))
      expect_true(dbHasCompleted(result))

      assign('request.count', 0, envir=environment(httr::GET))
      result <- dbSendQuery(conn, 'SELECT "text" AS z')
      expect_true(dbIsValid(result))
      expect_message(
        v <- dbFetch(result),
        'GET call failed with error: .*\\((HTTP )*404\\).*, retrying.*'
      )
      expect_equal(v, data.frame(z="text", stringsAsFactors=FALSE))
      expect_true(dbIsValid(result))
      expect_true(dbHasCompleted(result))

      result <- dbSendQuery(conn, 'SELECT x FROM respond_with_404')
      expect_true(dbIsValid(result))
      expect_error(
        dbFetch(result),
        'There was a problem with the request'
      )
    }
  )
})

test_that('dbIsValid works with mock - dbClearResult', {
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
      expect_true(dbIsValid(result))
      expect_true(dbClearResult(result))
      expect_false(dbIsValid(result))
      expect_true(dbClearResult(result))
      expect_false(dbIsValid(result))

      result <- dbSendQuery(conn, 'SELECT 2')
      expect_false(dbClearResult(result), label='DELETE fails')
      expect_true(dbIsValid(result))

      result <- dbSendQuery(conn, 'SELECT 3')
      expect_true(dbClearResult(result), label='complete query')
      expect_true(dbIsValid(result))
    }
  )
})
