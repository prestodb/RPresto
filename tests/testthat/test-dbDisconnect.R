# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dbDisconnect')

source('utilities.R')

test_that('dbDisconnect works with live database', {
  conn <- setup_live_connection()
  expect_true(dbDisconnect(conn))
})

test_that('dbDisconnect works with mock', {
  conn <- setup_mock_connection()
  expect_true(dbDisconnect(conn))
})
