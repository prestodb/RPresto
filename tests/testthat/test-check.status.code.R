# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('check.status.code')

source('utilities.R')

check.status.code <- RPresto:::check.status.code

test_that('check.status.code works', {
  response <- mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=400,
      extra_content=list('failed query')
  )[['response']]
  expect_error(check.status.code(response), '"failed query"')

  response <- mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=200
  )[['response']]
  expect_equal(check.status.code(response), invisible())

  response <- structure(
    list(
      url='dummy_url',
      status_code=403,
      headers=list('content-type'='application/json'),
      content=charToRaw('')
    ),
    class='response'
  )
  expect_error(
    check.status.code(response),
    '403'
  )
})
