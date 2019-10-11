# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('dbExistsTable')

source('utilities.R')

test_that('dbExistsTable works with live database', {
  conn <- setup_live_connection()
  expect_false(dbExistsTable(conn, '_non_existent_table_'))
})

test_that('dbExistsTable works with mock', {
  conn <- setup_mock_connection()
  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body="SHOW TABLES LIKE '_non_existent_table_'",
        next_uri='http://localhost:8000/query_1/1'
      ),
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body="SHOW TABLES LIKE 'existing_table'",
        next_uri='http://localhost:8000/query_2/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(Table='', stringsAsFactors=FALSE)[FALSE, ],
        state='FINISHED'
      ),
      mock_httr_response(
        'http://localhost:8000/query_2/1',
        status_code=200,
        data=data.frame(Table='existing_table', stringsAsFactors=FALSE),
        state='FINISHED'
      )
    ),
    {
      expect_false(dbExistsTable(conn, '_non_existent_table_'))
      expect_true(dbExistsTable(conn, 'existing_table'))
    })
})
