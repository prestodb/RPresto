# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(".stop.with.error.message")

source("utilities.R")

.stop.with.error.message <- RPresto:::.stop.with.error.message

test_that(".stop.with.error.message works", {
  content <- RPresto:::.response.to.content(
    mock_httr_response(
      "dummy_url",
      state = "dummy_state",
      status_code = 0,
      extra_content = list(
        error = list(message = jsonlite::unbox("Failure message"))
      )
    )[["response"]]
  )
  expect_error(
    .stop.with.error.message(content),
    "Query dummy_url failed: Failure message"
  )
})
