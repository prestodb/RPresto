# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbDisconnect"))

test_that("dbDisconnect works with live database", {
  conn <- setup_live_connection()
  expect_true(dbDisconnect(conn))
})

test_that("dbDisconnect works with mock", {
  conn <- setup_mock_connection()
  expect_true(dbDisconnect(conn))
})
