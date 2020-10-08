# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

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
        next_uri='http://localhost:8000/query_2/1',
        query_id='query_2'
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
        url='http://localhost:8000/v1/query/query_2',
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
        request_body='^SELECT \\* FROM __non_existent_table__$',
        next_uri='http://localhost:8000/query_2/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body=paste0(
          '^SELECT \\* FROM \\(SELECT \\* FROM __non_existent_table__\\) ',
          'WHERE 1 = 0$'
        ),
        next_uri='http://localhost:8000/query_4/1',
        query_id='query_4'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT \\* FROM empty_table',
        next_uri='http://localhost:8000/query_3/1',
        query_id='query_4'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='PLANNING',
        request_body='^SELECT \\* FROM three_columns$',
        next_uri='http://localhost:8000/query_5/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='PLANNING',
        request_body=paste0(
          '^SELECT \\* FROM \\(SELECT \\* FROM three_columns\\) ',
          'WHERE 1 = 0$'
        ),
        next_uri='http://localhost:8000/query_6/1',
        query_id='query_6'
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
      ),
      mock_httr_response(
        'http://localhost:8000/query_4/1',
        status_code=200,
        extra_content=list(error=list(
          message='Table __non_existent_table__ does not exist'
        )),
        state='FAILED',
      ),
      mock_httr_response(
        'http://localhost:8000/query_5/1',
        status_code=200,
        next_uri='http://localhost:8000/query_5/2',
        state='PLANNING',
      ),
      mock_httr_response(
        'http://localhost:8000/query_6/1',
        status_code=200,
        next_uri='http://localhost:8000/query_6/2',
        state='PLANNING',
      ),
      mock_httr_response(
        'http://localhost:8000/query_6/2',
        status_code=200,
        data=data.frame(
          a=1, b=TRUE, c='', stringsAsFactors=FALSE
        )[FALSE, , drop=FALSE],
        state='FINISHED',
      )
    ),
    `httr::DELETE`=mock_httr_replies(
      mock_httr_response(
        url='http://localhost:8000/v1/query_3',
        status_code=200,
        state=''
      ),
      mock_httr_response(
        url='http://localhost:8000/v1/query/query_4',
        status_code=200,
        state=''
      ),
      mock_httr_response(
        url='http://localhost:8000/v1/query/query_6',
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

      result <- dbSendQuery(conn, 'SELECT * FROM three_columns')
      expect_equal(dbListFields(result), c('a', 'b', 'c'))
    }
  )
})

test_that('dbListFields works with mock - PrestoResult - POST data', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT \\* FROM two_columns',
        data=data.frame(column1=3, column2=4)
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='SELECT \\* FROM two_rows',
        data=data.frame(column1=5, column2=6),
        next_uri='http://localhost:8000/query_2/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_2/1',
        status_code=200,
        data=data.frame(column1=7, column2=8),
        state='FINISHED',
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT * FROM two_columns')
      expect_true(dbIsValid(result))
      expect_equal(dbListFields(result), c('column1', 'column2'))
      expect_equal(dbFetch(result), data.frame(column1=3, column2=4))
      expect_true(dbHasCompleted(result))

      result <- dbSendQuery(conn, 'SELECT * FROM two_rows')
      expect_true(dbIsValid(result))
      expect_equal(dbListFields(result), c('column1', 'column2'))
      expect_equal(dbFetch(result), data.frame(column1=5, column2=6))
      expect_false(dbHasCompleted(result))
      expect_equal(dbFetch(result), data.frame(column1=7, column2=8))
      expect_true(dbHasCompleted(result))
    }
  )
})

test_that('dbListFields works with mock - PrestoResult - POST columns', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      list(
        url='http://localhost:8000/v1/statement',
        response=structure(
          list(
            url='http://localhost:8000/v1/statement',
            status_code=200,
            headers=list(
              'content-type'='application/json'
            ),
            content=charToRaw(jsonlite::toJSON(
              list(
                stats=list(state=jsonlite::unbox('QUEUED')),
                id=jsonlite::unbox('http__localhost_8000_v1_statement'),
                nextUri=jsonlite::unbox('http://localhost:8000/query_1/1'),
                columns=data.to.list(
                  data.frame(column1=1, column2=2)
                )[['column.data']]
              ),
              dataframe='values'
            ))
          ),
          class='response'
        ),
        request_body='SELECT \\* FROM two_columns'
      ),
      list(
        url='http://localhost:8000/v1/statement',
        response=structure(
          list(
            url='http://localhost:8000/v1/statement',
            status_code=200,
            headers=list(
              'content-type'='application/json'
            ),
            content=charToRaw(jsonlite::toJSON(
              list(
                stats=list(state=jsonlite::unbox('QUEUED')),
                id=jsonlite::unbox('http__localhost_8000_v1_statement'),
                nextUri=jsonlite::unbox('http://localhost:8000/query_2/1'),
                columns=data.to.list(
                  data.frame(column3=1, column4=2)
                )[['column.data']]
              ),
              dataframe='values'
            ))
          ),
          class='response'
        ),
        request_body='SELECT \\* FROM other_two_columns'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(column1=9, column2=10),
        state='FINISHED',
      ),
      list(
        url='http://localhost:8000/query_2/1',
        response=structure(
          list(
            url='http://localhost:8000/query_2/1',
            status_code=200,
            headers=list(
              'content-type'='application/json'
            ),
            content=charToRaw(jsonlite::toJSON(
              list(
                stats=list(state=jsonlite::unbox('QUEUED')),
                id=jsonlite::unbox('http__localhost_8000_query_1_1'),
                nextUri=jsonlite::unbox('http://localhost:8000/query_2/2'),
                columns=data.to.list(
                  data.frame(column3=1, column4=2)
                )[['column.data']]
              ),
              dataframe='values'
            ))
          ),
          class='response'
        )
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/2',
        status_code=200,
        data=data.frame(column3=13, column4=14),
        state='FINISHED',
      )
    ),
    {
      result <- dbSendQuery(conn, 'SELECT * FROM two_columns')
      expect_true(dbIsValid(result))
      expect_equal(dbListFields(result), c('column1', 'column2'))
      expect_equal(dbFetch(result), data.frame(column1=9, column2=10))
      expect_true(dbHasCompleted(result))

      result <- dbSendQuery(conn, 'SELECT * FROM other_two_columns')
      expect_true(dbIsValid(result))
      expect_equal(dbListFields(result), c('column3', 'column4'))
      expect_equal(
        dbFetch(result),
        data.frame(column3=1, column4=2)[FALSE, , drop=FALSE]
      )
      expect_false(dbHasCompleted(result))
      expect_equal(dbFetch(result), data.frame(column3=13, column4=14))
      expect_true(dbHasCompleted(result))
    }
  )
})
