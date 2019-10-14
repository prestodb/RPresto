# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('.extract.data')

source('utilities.R')


with_locale(test.locale(), test_that)('extract.data works', {
  .extract.data <- RPresto:::.extract.data

  response <- mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=0,
      data=data.frame()
    )[['response']]
  content <- RPresto:::response.to.content(response)

  dt <- .extract.data(content, timezone=test.timezone())
  expect_equal(
    .extract.data(content, timezone=test.timezone()),
    data.frame(),
    label='zero row, zero columns'
  )

  # We need to remove the tzone attribute otherwise the mocker treats it
  # as a timestamp with time zone column
  d <- data.frame.with.all.classes()
  attr(d[['POSIXct_no_time_zone']], 'tzone') <- NULL

  # We cannot directly send the zero row version of 'd', since then type
  # inference for list() columns do not work. So we send the full data,
  # then set the list form to NULL in the content
  response <- mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=0,
      data=d,
    )[['response']]
  content <- RPresto:::response.to.content(response)
  content[['data']] <- NULL

  # We need to remove the timezone from the column since without any
  # data json.tabular.to.data.frame has no way of finding it
  ev <- data.frame.with.all.classes()[FALSE, ]
  attr(ev[['POSIXct_with_time_zone']], 'tzone') <- NULL

  expect_equal_data_frame(
    .extract.data(content, timezone=test.timezone()),
    ev,
    label='Zero row, multiple columns'
  )

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

test_that('extract.data works without column information', {
  # Sometimes Presto does not return any column information for a chunk,
  # see https://github.com/prestodb/RPresto/issues/49
  .extract.data <- RPresto:::.extract.data
  expect_equal_data_frame(
    .extract.data(NULL, timezone=test.timezone()),
    data.frame()
  )
})
