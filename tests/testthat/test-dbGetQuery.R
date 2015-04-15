# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbGetQuery')

source('utilities.R')

test_that('dbGetQuery works with live database', {
  conn <- setup_live_connection()
  expect_equal(
    dbGetQuery(conn, 'SELECT n FROM (VALUES (1), (2)) AS t (n)'),
    data.frame(n=c(1,2))
  )

  expect_equal(
    dbGetQuery(conn, "SELECT t FROM (VALUES ('çğıöşü'), ('ÇĞİÖŞÜ')) AS a (t)"),
    data.frame(t=c('çğıöşü', 'ÇĞİÖŞÜ'), stringsAsFactors=FALSE)
  )
})

test_that('dbGetQuery works with mock', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT n FROM two_rows',
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT t FROM encoding_test',
        next_uri='http://localhost:8000/query_2/1'
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
        status_code=200,
        data=data.frame(t='çğıöşü', stringsAsFactors=FALSE),
        state='FINISHED',
        next_uri='http://localhost:8000/query_2/2'
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/2',
        status_code=200,
        data=data.frame(t='ÇĞİÖŞÜ', stringsAsFactors=FALSE),
        state='FINISHED'
      )
    ),
    {
      expect_equal(
        dbGetQuery(conn, 'SELECT n FROM two_rows'),
        data.frame(n=c(1,2))
      )
      expect_equal(
        dbGetQuery(conn, "SELECT t FROM encoding_test"),
        data.frame(t=c('çğıöşü', 'ÇĞİÖŞÜ'), stringsAsFactors=FALSE)
      )
    }
  )
})

