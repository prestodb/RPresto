# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbConnect')

test_that('dbConnect constructs PrestoConnection correctly', {
  expect_error(dbConnect(RPresto::Presto()), label='not enough arguments')

  expect_error(
    dbConnect(
      RPresto::Presto(),
      catalog='jmx'
    ),
    'argument ".*" is missing, with no default',
    label='not enough arguments'
  )

  expect_error(
    dbConnect(
      RPresto::Presto(),
      catalog='jmx',
      schema='test',
      host='http://localhost',
      port='',
      user=Sys.getenv('USER')
    ),
    'Please specify a port as an integer',
    label='invalid port'
  )

  with_mock(
    `httr::POST`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/v1/statement',
        status_code=200,
        state='QUEUED',
        request_body='SELECT CURRENT_TIMEZONE() AS connection_timezone',
        next_uri='http://localhost:8000/query_1/1'
      )
    ),
    `httr::GET`=mock_httr_replies(
      mock_httr_response(
        'http://localhost:8000/query_1/1',
        status_code=200,
        data=data.frame(
          connection_timezone=test.timezone(),
          stringsAsFactors=FALSE
        ),
        state='FINISHED'
      )
    ),
    {
      expect_is(
        dbConnect(
          RPresto::Presto(),
          catalog='jmx',
          schema='test',
          host='http://localhost',
          port=8000,
          session.timezone=test.timezone(),
          user=Sys.getenv('USER')
        ),
        'PrestoConnection'
      )

      expect_is(
        dbConnect(
          RPresto::Presto(),
          catalog='jmx',
          schema='test',
          host='http://localhost',
          port=8000,
          session.timezone=test.timezone(),
          user=Sys.getenv('USER'),
          parameters=list(
            experimental_big_query='true'
          )
        ),
        'PrestoConnection',
        label='extra parameters'
      )
    }
  )
})
