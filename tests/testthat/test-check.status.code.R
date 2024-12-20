# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(".check.response.status")

.check.response.status <- RPresto:::.check.response.status

test_that(".check.response.status works", {
  response <- mock_httr_response(
    "dummy_url",
    state = "dummy_state",
    status_code = 400,
    extra_content = list("failed query")
  )[["response"]]
  expect_error(.check.response.status(response), '"failed query"')

  response <- mock_httr_response(
    "dummy_url",
    state = "dummy_state",
    status_code = 200
  )[["response"]]
  expect_equal(.check.response.status(response), invisible())

  response <- structure(
    list(
      url = "dummy_url",
      status_code = 403,
      headers = list("content-type" = "application/json"),
      content = charToRaw("")
    ),
    class = "response"
  )
  expect_error(
    .check.response.status(response),
    "403",
    class = "http_403"
  )
})
