# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('dbGetInfo')

source('utilities.R')

test_that('dbGetInfo works with live database', {
  conn <- setup_live_connection()
  connection.info <- dbGetInfo(conn)
  expect_is(connection.info, 'list')
  expect_equal(length(connection.info), 5)
  expect_true(setequal(
    names(connection.info),
    c('host', 'port', 'user', 'catalog', 'schema')
  ))

  result <- dbSendQuery(conn, 'SELECT 1')
  result.info <- dbGetInfo(result)
  expect_equal(
    result.info[c('statement', 'row.count', 'has.completed')],
    list(
      statement='SELECT 1',
      row.count=as.integer(0),
      has.completed=FALSE
    )
  )
  expect_is(result.info[['stats']], 'list')
  expect_false(is.null(result.info[['stats']][['state']]))
  expect_true(dbClearResult(result))
})

test_that('dbGetInfo works with mock', {
  conn <- setup_mock_connection()
  connection.info <- dbGetInfo(conn)
  expect_equal(
    dbGetInfo(conn),
    list(
      host='http://localhost',
      port=as.integer(8000),
      user=Sys.getenv('USER'),
      catalog='catalog',
      schema='test'
    )
  )
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT n FROM two_rows',
        next_uri='http://localhost:8000/query_1/1',
        query_id='query_1'
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
      expect_equal(
        dbGetInfo(result),
        list(
          query.id='query_1',
          statement='SELECT n FROM two_rows',
          row.count=as.integer(0),
          has.completed=FALSE,
          stats=list(state='QUEUED')
        )
      )
      expect_equal(dbFetch(result), data.frame(n=1))
      expect_equal(
        dbGetInfo(result),
        list(
          query.id='query_1',
          statement='SELECT n FROM two_rows',
          row.count=as.integer(1),
          has.completed=FALSE,
          stats=list(state='FINISHED')
        )
      )
      expect_equal(dbFetch(result), data.frame(n=2))
      expect_equal(
        dbGetInfo(result),
        list(
          query.id='query_1',
          statement='SELECT n FROM two_rows',
          row.count=as.integer(2),
          has.completed=TRUE,
          stats=list(state='FINISHED')
        )
      )
    }
  )
})
