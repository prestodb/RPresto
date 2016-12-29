# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbListFields')

source('utilities.R')

test_that('dbListFields works with live database', {
  conn <- setup_live_connection()

  expect_error(dbListFields(conn, '__non_existent_table__'))

  result <- dbSendQuery(conn, 'SELECT 1 AS n, 2 AS n2')
  expect_equal(dbListFields(result), c('n', 'n2'))
  expect_true(dbClearResult(result))

  result <- dbSendQuery(conn, 'SELECT * FROM __non_existent_table__')
  expect_error(
    dbListFields(result),
    paste0(
      'Query.*failed: (line [0-9:]+ )?',
      'Table .*__non_existent_table__ does not exist'
    )
  )
  expect_true(dbClearResult(result))
})

test_that('dbListFields works with mock - PrestoConnection', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT \\* FROM "two_columns" LIMIT 0',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT \\* FROM "__non_existent_table__" LIMIT 0',
        next_uri='http://localhost:8000/query_2/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(
          column1=1,
          column2=FALSE,
          stringsAsFactors=FALSE
        )[FALSE, ],
        state='FINISHED',
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/1',
        status_code=200,
        state='FAILED',
      )
    ),
    `httr::DELETE`=mock_httr_replies(
      mock_httr_response(
        url='http://localhost:8000/query_2/1',
        status_code=200,
        state=''
      )
    ),
    {
      expect_equal(dbListFields(conn, 'two_columns'), c('column1', 'column2'))
      expect_error(
        dbListFields(conn, '__non_existent_table__'),
        'Query .* failed'
      )
    }
  )
})

test_that('dbListFields works with mock - PrestoResult', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT \\* FROM two_columns',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT \\* FROM __non_existent_table__',
        next_uri='http://localhost:8000/query_2/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT \\* FROM empty_table',
        next_uri='http://localhost:8000/query_3/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(column1=1, column2=2),
        state='FINISHED',
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/1',
        status_code=200,
        extra_content=list(error=list(
          message='Table __non_existent_table__ does not exist'
        )),
        state='FAILED',
      ),
      mock_httr_response(
        'http://localhost:8000/query_3/1',
        status_code=200,
        state='FINISHED',
      )
    ),
    `httr::DELETE`=mock_httr_replies(
      mock_httr_response(
        url='http://localhost:8000/query_3/1',
        status_code=200,
        state=''
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT * FROM two_columns')
      expect_true(dbIsValid(result))
      expect_equal(dbListFields(result), c('column1', 'column2'))

      result <- dbSendQuery(conn, 'SELECT * FROM __non_existent_table__')
      expect_true(dbIsValid(result))
      expect_error(
        dbListFields(result),
        'Query.*failed: Table __non_existent_table__ does not exist'
      )

      result <- dbSendQuery(conn, 'SELECT * FROM empty_table')
      expect_equal(dbListFields(result), character(0))

      result <- dbSendQuery(conn, 'SELECT * FROM empty_table')
      expect_true(dbClearResult(result))
      expect_error(
        dbListFields(result),
        'The result object is not valid'
      )
    }
  )
})

