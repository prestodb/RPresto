# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('.extract.data')

source('utilities.R')

.extract.data <- RPresto:::.extract.data

test_that('extract.data works', {
  expect_equal(.extract.data(list()), data.frame())

  content <- RPresto:::response.to.content(
    mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=0,
      data=data.frame.with.all.classes()
    )[['response']]
  )
  expect_equal_data_frame(
    .extract.data(content, timezone=test.timezone()),
    data.frame.with.all.classes()
  )
})
