# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("sql_escape_date")

with_locale(test.locale(), test_that)("as() works", {
  dbplyr_version <- try(as.character(utils::packageVersion("dbplyr")))
  if (inherits(dbplyr_version, "try-error")) {
    skip("dbplyr not available")
  } else if (utils::compareVersion(dbplyr_version, "1.4.3") < 0) {
    skip("sql_escape_date not available in dbplyr < 1.4.3")
  }
  s <- setup_mock_dplyr_connection()[["db"]]

  expect_equal(
    dbplyr::sql_escape_date(as.Date("2020-03-22"), con = s[["con"]]),
    "DATE '2020-03-22'"
  )
})
