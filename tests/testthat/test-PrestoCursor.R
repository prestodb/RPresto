# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('PrestoCursor')

source('utilities.R')

test_that('PrestoCursor methods work correctly', {
    post.content <- list(stats=list(state='INITIAL'))

    cursor <- PrestoCursor(post.content)
    expect_equal(cursor$infoUri(), '')
    expect_equal(cursor$nextUri(), '')
    expect_equal(cursor$state(), 'INITIAL')
    expect_equal(cursor$fetchedRowCount(), 0)
    expect_equal(cursor$stats(), list(state='INITIAL'))
    expect_true(cursor$hasCompleted())

    cursor$state('__TEST')
    expect_equal(cursor$state(), '__TEST')

    uri <- 'http://localhost:8000/query_1/1'
    r <- mock_httr_response(
      url='http://localhost:8000/v1/statement',
      status_code=200,
      state='RUNNING',
      next_uri=uri,
      info_uri='http://localhost:8000/v1/query/query_1'
    )
    content <- RPresto:::response.to.content(r[['response']])
    cursor$updateCursor(content, 1)

    expect_equal(
      cursor$infoUri(),
      'http://localhost:8000/v1/query/query_1'
    )
    expect_equal(cursor$nextUri(), uri)
    expect_equal(cursor$state(), 'RUNNING')
    expect_equal(cursor$fetchedRowCount(), 1)
    expect_is(cursor$stats(), 'list')
    expect_false(cursor$hasCompleted())

    r <- mock_httr_response(
      url=uri,
      status_code=200,
      info_uri='http://localhost:8000/v1/query/query_1_updated',
      state='FINISHED'
    )
    content <- RPresto:::response.to.content(r[['response']])
    cursor$updateCursor(content, 1)

    expect_equal(
      cursor$infoUri(),
      'http://localhost:8000/v1/query/query_1_updated'
    )
    expect_equal(cursor$nextUri(), '')
    expect_equal(cursor$state(), 'FINISHED')
    expect_equal(cursor$fetchedRowCount(), 2)
    expect_is(cursor$stats(), 'list')
    expect_true(cursor$hasCompleted())
})
