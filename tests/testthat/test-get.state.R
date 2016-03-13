# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('get.state')

source('utilities.R')


with_locale(test.locale(), test_that)(
  'get.state works', {

  get.state <- RPresto:::get.state

  content <- RPresto:::response.to.content(
    mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=0,
      data=data.frame.with.all.classes()
    )[['response']]
  )
  expect_equal(get.state(content), 'dummy_state')
  expect_error(get.state(list()), 'No state information in content')
  expect_error(get.state(list(stats=list())), 'No state information in content')
})
