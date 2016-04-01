# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbFetch')

source('utilities.R')

test_that('dbFetch works with live database', {
  conn <- setup_live_connection()

  result <- dbSendQuery(conn, 'SELECT 1 AS n')
  expect_error(
    dbFetch(result, 1),
    '.*fetching custom number of rows.*is not supported.*'
  )
  expect_equal(dbFetch(result, -1), data.frame(n=1))
  expect_true(dbHasCompleted(result))

  result <- dbSendQuery(conn, 'SELECT 2 AS n')
  expect_true(dbClearResult(result))
  expect_error(
    dbFetch(result, -1),
    '.*Result object is not valid.*'
  )

  result <- dbSendQuery(conn, 'SELECT 3 AS n LIMIT 0')
  rv <- dbFetch(result, -1)
  expect_equal(rv, data.frame(n=1L)[FALSE, , drop=FALSE])
})

test_that('dbFetch works with mock', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT n FROM (VALUES (1), (2)) AS t (n)',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 2',
        next_uri='http://localhost:8000/query_2/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 3',
        next_uri='http://localhost:8000/query_3/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT 4 LIMIT 0',
        next_uri='http://localhost:8000/query_4/1'
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
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/1',
        status_code=400,
        data='Broken URL',
        state='FAILED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_3/1',
        status_code=200,
        data='Failed URL',
        state='FAILED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/1',
        status_code=200,
        data=data.frame('_col0'=4)[FALSE, , drop=FALSE],
        state='FINISHED'
      )
    ),
    `httr::DELETE`=mock_httr_replies(
      mock_httr_response(
        url='http://localhost:8000/query_1/2',
        status_code=200,
        state=''
      )
    ),
    `httr::handle_reset`=function(...) return(),
    {
      result <- dbSendQuery(conn, "SELECT n FROM (VALUES (1), (2)) AS t (n)")
      expect_error(
        dbFetch(result, 1),
        '.*fetching custom number of rows.*is not supported.*'
      )
      expect_equal(dbFetch(result), data.frame(n=1L))
      expect_true(dbClearResult(result))
      expect_error(
        dbFetch(result),
        '.*Result object is not valid.*'
      )

      result <- dbSendQuery(conn, "SELECT n FROM (VALUES (1), (2)) AS t (n)")
      expect_equal(dbGetRowCount(result), 0)
      expect_equal(dbFetch(result), data.frame(n=1L))
      expect_equal(dbGetRowCount(result), 1)
      expect_equal(dbFetch(result), data.frame(n=2L))
      expect_equal(dbGetRowCount(result), 2)
      expect_true(dbHasCompleted(result))

      result <- dbSendQuery(conn, "SELECT n FROM (VALUES (1), (2)) AS t (n)")
      expect_equal(dbFetch(result, -1), data.frame(n=c(1L, 2L)))
      expect_equal(dbGetRowCount(result), 2)

      result <- dbSendQuery(conn, 'SELECT 2')
      expect_error(
        dbFetch(result), 
        paste0('Cannot fetch .*, error: ',
            'There was a problem with the request and we have exhausted ',
            'our retry limit')
      )

      result <- dbSendQuery(conn, 'SELECT 3')
      expect_error(
        dbFetch(result), 
        '.*Query .*localhost.8000.query.3.1 failed.*'
      )

      result <- dbSendQuery(conn, 'SELECT 4 LIMIT 0')
      rv <- dbFetch(result, -1)
      ev <- data.frame('_col0'=1L)[FALSE, , drop=FALSE]
      expect_equal_data_frame(rv, ev)
    }
  )
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 3',
        next_uri='http://localhost:8000/query_3/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='RUNNING',
        request_body='SELECT 4',
        next_uri='http://localhost:8000/query_4/1'
      )
    ),
    `httr::GET`=function(url, ...) {
      if (url == 'http://localhost:8000/query_3/1') {
        stop('Error')
      }
      if (request.count == 0) {
        request.count <<- 1
        stop('First request is an error')
      }
      return(mock_httr_replies(
        mock_httr_response(
          'http://localhost:8000/query_4/1',
          status_code=200,
          state='FINISHED',
          data=data.frame(n=4)
        )
      )(url))
    },
    `httr::handle_reset`=function(...) return(),
    {
      result <- dbSendQuery(conn, 'SELECT 3')
      expect_error(
        suppressMessages(dbFetch(result)),
        paste0(
          'Cannot fetch .*, error: ',
          'There was a problem with the request and we have exhausted our ',
          'retry limit'
        )
      )

      assign('request.count', 0, envir=environment(httr::GET))
      result <- dbSendQuery(conn, 'SELECT 4')
      expect_message(
        v <- dbFetch(result),
        'First request is an error'
      )
      expect_equal(v, data.frame(n=4))
    }
  )
})

with_locale(test.locale(), test_that)('dbFetch rbind works correctly', {
  conn <- setup_mock_connection()

  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT * FROM all_types',
        next_uri='http://localhost:8000/query_1/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        next_uri='http://localhost:8000/query_1/2',
        data=data.frame.with.all.classes(1),
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_1/2',
        status_code=200,
        data=data.frame.with.all.classes(2),
        state='FINISHED'
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT * FROM all_types')

      expect_equal_data_frame(
        dbFetch(result, -1),
        data.frame.with.all.classes()
      )
    }
  )
})

with_locale(test.locale(), test_that)('dbFetch rbind works with zero row chunks', {
  conn <- setup_mock_connection()
  data <- data.frame(
    integer=c(1L, 2L),
    double=c(3.0, 4),
    logical=c(TRUE, NA)
  )
  full.data <- data.frame.with.all.classes()
  full.data <- full.data[
    ,
    -match(c('raw', 'list_unnamed', 'list_named'), colnames(full.data)),
    drop=FALSE
  ]
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT integer, double, logical FROM all_types',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='PAUSED',
        request_body='SELECT double FROM all_types',
        next_uri='http://localhost:8000/query_2/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='PAUSED',
        request_body='SELECT * FROM all_types',
        next_uri='http://localhost:8000/query_3/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT * FROM all_types_with_queue',
        next_uri='http://localhost:8000/query_4/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        next_uri='http://localhost:8000/query_1/2',
        data=data[1, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_1/2',
        status_code=200,
        next_uri='http://localhost:8000/query_1/3',
        data=data[FALSE, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_1/3',
        status_code=200,
        data=data[2, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/1',
        status_code=200,
        next_uri='http://localhost:8000/query_2/2',
        data=data[1, 'double', drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/2',
        status_code=200,
        next_uri='http://localhost:8000/query_2/3',
        data=data[FALSE, 'double', drop=FALSE],
        state='RUNNING'
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/3',
        status_code=200,
        data=data[2, 'double', drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_3/1',
        status_code=200,
        next_uri='http://localhost:8000/query_3/2',
        data=full.data[FALSE, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_3/2',
        status_code=200,
        data=full.data,
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/1',
        status_code=200,
        next_uri='http://localhost:8000/query_4/2',
        state='QUEUED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/2',
        status_code=200,
        next_uri='http://localhost:8000/query_4/3',
        state='QUEUED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/3',
        next_uri='http://localhost:8000/query_4/4',
        status_code=200,
        data=full.data[1, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/4',
        status_code=200,
        next_uri='http://localhost:8000/query_4/5',
        state='QUEUED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/5',
        next_uri='http://localhost:8000/query_4/6',
        status_code=200,
        data=full.data[FALSE, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/6',
        next_uri='http://localhost:8000/query_4/7',
        status_code=200,
        data=full.data[2, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/7',
        next_uri='http://localhost:8000/query_4/8',
        status_code=200,
        data=full.data[FALSE, , drop=FALSE],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/8',
        status_code=200,
        state='FINISHED'
      )
    ),
    {
      result <- dbSendQuery(
        conn,
        'SELECT integer, double, logical FROM all_types'
      )

      expect_equal_data_frame(
        dbFetch(result, -1),
        data,
        label='multiple columns'
      )

      result <- dbSendQuery(conn, 'SELECT double FROM all_types')

      expect_equal_data_frame(
        dbFetch(result, -1),
        data[, 'double', drop=FALSE],
        label='single column'
      )

      result <- dbSendQuery(conn, 'SELECT * FROM all_types')

      expect_equal_data_frame(
        dbFetch(result, -1),
        full.data,
        label='zero chunk first'
      )

      result <- dbSendQuery(conn, 'SELECT * FROM all_types_with_queue')
      expect_equal_data_frame(
        dbFetch(result, -1),
        full.data,
        label='with empty chunks'
      )
    }
  )
})
