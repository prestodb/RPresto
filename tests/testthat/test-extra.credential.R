# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("extra.credentials")

source("utilities.R")

test_that("extra.credentials should be set in the connection as a string", {
  conn <- setup_live_connection(extra.credentials = "test.token.foo=bar")
  expect_equal(conn@extra.credentials, "test.token.foo=bar")
})
