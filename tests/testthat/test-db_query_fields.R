# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('db_query_fields')

source('utilities.R')

test_that('db_query_fields works with live database', {
  s <- setup_live_dplyr_connection()

  fields <- dplyr::db_query_fields(
    s[['db']][['con']],
    dplyr::sql_subquery(
      s[['db']][['con']],
      dplyr::sql("SELECT 1 AS a, 'text' AS b")
    )
  )
  expect_equal(fields, c('a', 'b'))

  expect_equal(
    dplyr::db_query_fields(
      s[['db']][['con']],
      dplyr::ident(s[['iris_table_name']])
    ),
    c("sepal_length", "sepal_width", "petal_length", "petal_width", "species")
  )

  expect_error(
    dplyr::db_query_fields(
      s[['db']][['con']],
      dplyr::ident('__non_existent_table__')
    ),
    "Query.*failed:.*Table .*__non_existent_table__ does not exist"
  )
})

test_that('db_query_fields works with mock', {
  s <- setup_mock_dplyr_connection()[['db']]
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body=
          '^SELECT \\* FROM \\(SELECT 1 AS a, \'t\' AS b\\) "a" LIMIT 0$',
        next_uri='http://localhost:8000/query_1/1',
        info_uri='http://localhost:8000/v1/query/query_1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        # For dplyr 0.4.3
        request_body=paste0(
          '^SELECT \\* FROM ',
          '\\(\\(SELECT 1 AS a, \'t\' AS b\\) AS "a"\\) ',
          'AS "zzz[0-9]+" LIMIT 0$'
        ),
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        # For dplyr 0.5.0
        request_body=paste0(
          '^SELECT \\* FROM \\(',
            '\\(SELECT 1 AS a, \'t\' AS b\\) "a"',
          '\\) "zzz[0-9]+" WHERE 1 = 0$'
        ),
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='^SELECT \\* FROM "__non_existent_table__" LIMIT 0$',
        next_uri='http://localhost:8000/query_2/1',
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        # For dplyr 0.5.0
        request_body=paste0(
          '^SELECT \\* FROM "__non_existent_table__" ',
          'AS "zzz[0-9]+" WHERE 1 = 0$'
        ),
        next_uri='http://localhost:8000/query_2/1',
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        request_body='^SELECT \\* FROM "empty_table" LIMIT 0$',
        next_uri='http://localhost:8000/query_3/1',
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='FINISHED',
        # For dplyr 0.5.0
        request_body=paste0(
          '^SELECT \\* FROM "empty_table" AS "zzz[0-9]+"',
          ' WHERE 1 = 0$'
        ),
        next_uri='http://localhost:8000/query_3/1',
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='^SELECT \\* FROM "two_columns" LIMIT 0$',
        next_uri='http://localhost:8000/query_4/1',
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        # For dplyr 0.5.0
        request_body=paste0(
          '^SELECT \\* FROM "two_columns" AS "zzz[0-9]+"',
          ' WHERE 1 = 0$'
        ),
        next_uri='http://localhost:8000/query_4/1',
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(a=1, b='t', stringsAsFactors=FALSE),
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
        data=data.frame(a=1, b=TRUE),
        state='FINISHED',
      )
    ),
    `httr::DELETE`=function(url, body, ...) {
      mock_httr_response(
        url='http://localhost:8000/query_1/1',
        status_code=200,
        state=''
      )[['response']]
    },
    {
      fields <- dplyr::db_query_fields(
        s[['con']],
        dplyr::sql_subquery(
          s[['con']],
          dplyr::sql("SELECT 1 AS a, 't' AS b"),
          name='a'
        )
      )
      expect_equal(fields, c('a', 'b'))

      expect_equal(
        dplyr::db_query_fields(
          s[['con']],
          dplyr::ident('two_columns')
        ),
        c('a', 'b')
      )

      expect_equal(
        dplyr::db_query_fields(
          s[['con']],
          dplyr::ident('empty_table')
        ),
        character(0)
      )
      expect_error(
        dplyr::db_query_fields(
          s[['con']],
          dplyr::ident('__non_existent_table__')
        ),
        "Query.*failed:.*Table .*__non_existent_table__ does not exist"
      )
    }
  )
})

