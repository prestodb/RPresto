# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("request headers")

source("utilities.R")

test_that("kerberos configs can be generated", {
  config <- kerberos_configs()
  expect_true(
    all.equal(
      config,
      httr::config(
        httpauth = 4,
        userpwd = ":",
        service_name = "presto",
        ssl_verifypeer = FALSE,
        ssl_verifyhost = FALSE
      )
    )
  )
})
