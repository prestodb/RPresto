# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(".get.content.state")

with_locale(test.locale(), test_that)(
  ".get.content.state works", {
    .get.content.state <- RPresto:::.get.content.state

    content <- RPresto:::.response.to.content(
      mock_httr_response(
        "dummy_url",
        state = "dummy_state",
        status_code = 0,
        data = data.frame.with.all.classes()
      )[["response"]]
    )
    expect_equal(.get.content.state(content), "dummy_state")
    expect_error(.get.content.state(list()), "No state information in content")
    expect_error(.get.content.state(list(stats = list())), "No state information in content")
})
