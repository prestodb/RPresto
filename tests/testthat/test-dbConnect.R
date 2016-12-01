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

  expect_is(
    dbConnect(
      RPresto::Presto(),
      catalog='jmx',
      schema='test',
      host='http://localhost',
      port=8000,
      source='testsource',
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
      source='testsource',
      session.timezone=test.timezone(),
      user=Sys.getenv('USER'),
      parameters=list(
        experimental_big_query='true'
      )
    ),
    'PrestoConnection',
    label='extra parameters'
  )
})
