# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("translate_sql")

with_locale(test.locale(), test_that)("as() works", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("dplyr not available")
  }

  s <- setup_mock_dplyr_connection()[["db"]]

  expect_equal(
    dbplyr::translate_sql(as(x, 0.0), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS DOUBLE)')
  )
  expect_equal(
    dbplyr::translate_sql(pmax(x), con = s[["con"]]),
    dbplyr::sql('GREATEST("x")')
  )

  substituted.expression <- substitute(as(x, l), list(l = Sys.Date()))
  expect_equal(
    dbplyr::translate_sql_(list(substituted.expression), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS DATE)')
  )

  # Hacky dummy table so that we can test substitution
  s <- setup_live_dplyr_connection()[["db"]]
  if (requireNamespace("dbplyr", quietly = TRUE) &&
    utils::compareVersion(
      as.character(utils::packageVersion("dbplyr")),
      "1.3.0"
    ) >= 0
  ) {
    # vars argument gives a deprecation warning starting with 1.3.0
    t <- dplyr::tbl(
      s,
      from = dplyr::sql_subquery(
        s[["con"]],
        dbplyr::sql("SELECT 1 AS a")
      )
    )
  } else {
    t <- dplyr::tbl(
      s,
      from = dplyr::sql_subquery(
        s[["con"]],
        dbplyr::sql("SELECT 1")
      ),
      vars = c("x")
    )
  }

  l <- list(c(a = 1L))
  expect_equal(
    dbplyr::translate_sql(
      as(x, !!l),
      con = s[["con"]]
    ),
    dbplyr::sql('CAST("x" AS MAP<VARCHAR, INTEGER>)')
  )
  expect_equal(
    dbplyr::translate_sql(
      as(x, !!local(list(c(a = Sys.time())))),
      con = s[["con"]]
    ),
    dbplyr::sql('CAST("x" AS MAP<VARCHAR, TIMESTAMP>)')
  )
  r <- as.raw(0)
  p <- as.POSIXct("2001-02-03 04:05:06", tz = "Europe/Istanbul")
  l <- TRUE
  query <- as.character(dbplyr::sql_render(dplyr::transmute(
    t,
    b = as(a, r),
    c = as(a, p),
    d = as(a, l)
  )))
  old_expected_query_pattern <- paste0(
    '^SELECT "b"( AS "b")?, "c"( AS "c")?, "d"( AS "d")?.*',
    "FROM \\(",
    "SELECT ",
    '"_col0", ',
    'CAST\\("a" AS VARBINARY\\) AS "b", ',
    'CAST\\("a" AS TIMESTAMP\\) AS "c", ',
    'CAST\\("a" AS BOOLEAN\\) AS "d"\n',
    "FROM \\(",
    '\\(SELECT 1\\) "[_0-9a-z]+"',
    '\\) "[_0-9a-z]+"',
    '\\) "[_0-9a-z]+"$'
  )
  new_expected_query_pattern <- paste0(
    "^SELECT\\s*",
    'CAST\\("a" AS VARBINARY\\) AS "b",\\s*',
    'CAST\\("a" AS TIMESTAMP\\) AS "c",\\s*',
    'CAST\\("a" AS BOOLEAN\\) AS "d"\n',
    "FROM \\(",
    '\\(SELECT 1 AS a\\) "[_0-9a-z]+"',
    '\\) "[_0-9a-z]+"$'
  )
  # newer versions of dplyr have a simpler pattern
  expected_query_pattern <- paste0(
    "(",
    old_expected_query_pattern,
    "|",
    new_expected_query_pattern,
    ")"
  )
  expect_true(
    grepl(expected_query_pattern, query)
  )
})

with_locale(test.locale(), test_that)("as.<type>() works", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("dplyr not available")
  }

  s <- setup_mock_dplyr_connection()[["db"]]
  expect_equal(
    dbplyr::translate_sql(as.character(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS VARCHAR)')
  )
  expect_equal(
    dbplyr::translate_sql(as.numeric(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS DOUBLE)')
  )
  expect_equal(
    dbplyr::translate_sql(as.double(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS DOUBLE)')
  )
  expect_equal(
    dbplyr::translate_sql(as.integer(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS BIGINT)')
  )
  expect_equal(
    dbplyr::translate_sql(as.Date(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS DATE)')
  )
  expect_equal(
    dbplyr::translate_sql(as.logical(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS BOOLEAN)')
  )
  expect_equal(
    dbplyr::translate_sql(as.raw(x), con = s[["con"]]),
    dbplyr::sql('CAST("x" AS VARBINARY)')
  )
})

with_locale(test.locale(), test_that)("`[[` works for char/numeric indices", {
  dbplyr_version <- try(as.character(utils::packageVersion("dbplyr")))
  if (inherits(dbplyr_version, "try-error")) {
    skip("dbplyr not available")
  } else if (utils::compareVersion(dbplyr_version, "1.4.0") < 0) {
    skip("remote evaluation of `[[` requires dbplyr >= 1.4.0")
  }

  s <- setup_mock_dplyr_connection()[["db"]]

  # a character index should be escaped
  expect_equal(
    dbplyr::translate_sql(x[["a"]], con = s[["con"]]),
    dbplyr::sql("ELEMENT_AT(\"x\", 'a')")
  )
  # but a numeric index (for arrays) should not
  expect_equal(
    dbplyr::translate_sql(x[[1]], con = s[["con"]]),
    dbplyr::sql("ELEMENT_AT(\"x\", 1)")
  )
  expect_equal(
    dbplyr::translate_sql(x[[1L]], con = s[["con"]]),
    dbplyr::sql("ELEMENT_AT(\"x\", 1)")
  )

  # neither `x` nor `i` should be evaluated locally
  expect_equal(
    dbplyr::translate_sql(dim[["a"]], con = s[["con"]]),
    dbplyr::sql("ELEMENT_AT(\"dim\", 'a')")
  )
  expect_equal(
    dbplyr::translate_sql(x[["dim"]], con = s[["con"]]),
    dbplyr::sql("ELEMENT_AT(\"x\", 'dim')")
  )
})

with_locale(test.locale(), test_that)("`[[` works for dynamic indices", {
  dbplyr_version <- try(as.character(utils::packageVersion("dbplyr")))
  if (inherits(dbplyr_version, "try-error")) {
    skip("dbplyr not available")
  } else if (utils::compareVersion(dbplyr_version, "1.4.0") < 0) {
    skip("remote evaluation of `[[` requires dbplyr >= 1.4.0")
  }

  # create an inline table with both array and map columns
  x <- dplyr::tbl(
    setup_live_dplyr_connection()[["db"]],
    dbplyr::sql(
      "SELECT * FROM (
      VALUES
        (
          ARRAY['a', 'b'],
          MAP(ARRAY[1, 2], ARRAY['map_1', 'map_2']),
          2.0
        ),
        (
          ARRAY['x', 'y'],
          MAP(ARRAY[1.5, 2.5], ARRAY['map_1.5', 'map_2.5']),
          1.0
        )
    ) AS data(arr, map, idx)"
    )
  )

  # index an array with a function of fixed values
  expect_equal(
    x %>%
      dplyr::mutate(y = arr[[as.integer(POW(2, 1))]]) %>%
      dplyr::pull(y),
    c("b", "y")
  )

  # index an array with a function of a dynamic reference
  expect_equal(
    x %>%
      dplyr::mutate(y = arr[[as.integer(POW(idx, 1))]]) %>%
      dplyr::pull(y),
    c("b", "x")
  )

  # index a map with a function of fixed values
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[as.integer(POW(2, 1))]]) %>%
      dplyr::pull(y),
    c("map_2", NA)
  )

  # index a map with a function of a dynamic reference
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[as.integer(POW(idx, 1))]]) %>%
      dplyr::pull(y),
    c("map_2", NA)
  )

  # index an array with a fixed numeric
  expect_equal(
    x %>%
      dplyr::mutate(y = arr[[1]]) %>%
      dplyr::pull(y),
    c("a", "x")
  )

  # index an array with a fixed integer
  expect_equal(
    x %>%
      dplyr::mutate(y = arr[[1L]]) %>%
      dplyr::pull(y),
    c("a", "x")
  )

  # index a map with an integer-ish numeric
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[1]]) %>%
      dplyr::pull(y),
    c("map_1", NA)
  )

  # index a map with a numeric
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[1.5]]) %>%
      dplyr::pull(y),
    c(NA, "map_1.5")
  )

  # index a map with an integer
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[1L]]) %>%
      dplyr::pull(y),
    c("map_1", NA)
  )

  # index an array with a dynamic reference coerced to an integer
  expect_equal(
    x %>%
      dplyr::mutate(y = arr[[as.integer(idx)]]) %>%
      dplyr::pull(y),
    c("b", "x")
  )

  # index a map with an integer-ish dynamic reference
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[idx]]) %>%
      dplyr::pull(y),
    c("map_2", NA)
  )

  # index a map with a function of an integer-ish dynamic reference
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[idx + 0.5]]) %>%
      dplyr::pull(y),
    c(NA, "map_1.5")
  )

  # index a map with a dynamic reference coerced to an integer
  expect_equal(
    x %>%
      dplyr::mutate(y = map[[as.integer(idx)]]) %>%
      dplyr::pull(y),
    c("map_2", NA)
  )
})

with_locale(test.locale(), test_that)("is.[in]finite() works", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("dplyr not available")
  }

  s <- setup_mock_dplyr_connection()[["db"]]

  expect_equal(
    dbplyr::translate_sql(is.infinite(x), con = s$con),
    dbplyr::sql("IS_INFINITE(\"x\")")
  )

  expect_equal(
    dbplyr::translate_sql(is.finite(x), con = s$con),
    dbplyr::sql("IS_FINITE(\"x\")")
  )
})

with_locale(test.locale(), test_that)("quantile() and median() throw errors", {
  dbplyr_version <- try(as.character(utils::packageVersion("dbplyr")))
  if (inherits(dbplyr_version, "try-error")) {
    skip("dbplyr not available")
  } else if (utils::compareVersion(dbplyr_version, "1.4.0") < 0) {
    skip("remote evaluation of `[[` requires dbplyr >= 1.4.0")
  }

  s <- setup_mock_dplyr_connection()[["db"]]

  expect_error(dbplyr::translate_sql(quantile(x, 0.9), con = s[["con"]]))
  expect_error(dbplyr::translate_sql(median(x), con = s[["con"]]))

  x <- dplyr::tbl(
    setup_live_dplyr_connection()[["db"]],
    dbplyr::sql(
      "SELECT * FROM (
      VALUES
        ('a', 1),
        ('b', 2)
    ) AS data(y, z)"
    )
  )

  expect_error(dplyr::summarize(x, q = quantile(z, 0.9)) %>% collect())
  expect_error(dplyr::summarize(x, q = median(z)) %>% collect())

  # aggregate
  expect_error(
    x %>%
      dplyr::group_by(y) %>%
      dplyr::summarize(q = quantile(z, 0.9)) %>%
      collect()
  )

  expect_error(
    x %>%
      dplyr::group_by(y) %>%
      dplyr::summarize(q = median(z)) %>%
      collect()
  )

  # windowed
  expect_error(
    x %>%
      dplyr::group_by(y) %>%
      dplyr::mutate(q = quantile(z, 0.9)) %>%
      collect()
  )

  expect_error(
    x %>%
      dplyr::group_by(y) %>%
      dplyr::mutate(q = median(z)) %>%
      collect()
  )
})

with_locale(test.locale(), test_that)("first(), last(), and nth() work", {
  testthat::skip_if_not_installed("dbplyr", "2.4.0")
  s <- setup_mock_dplyr_connection()[["db"]]
  expect_equal(
    dbplyr::translate_sql(first(x, order_by = y), con = s[["con"]]),
    dbplyr::sql('FIRST_VALUE("x") OVER (ORDER BY "y")')
  )
  expect_equal(
    dbplyr::translate_sql(last(x, order_by = y), con = s[["con"]]),
    dbplyr::sql('LAST_VALUE("x") OVER (ORDER BY "y" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)')
  )
  expect_equal(
    dbplyr::translate_sql(nth(x, 2, order_by = y), con = s[["con"]]),
    dbplyr::sql('NTH_VALUE("x", 2) OVER (ORDER BY "y")')
  )
  expect_equal(
    dbplyr::translate_sql(first(x, order_by = y, na_rm = TRUE), con = s[["con"]]),
    dbplyr::sql('FIRST_VALUE("x") IGNORE NULLS OVER (ORDER BY "y")')
  )
  expect_equal(
    dbplyr::translate_sql(last(x, order_by = y, na_rm = TRUE), con = s[["con"]]),
    dbplyr::sql('LAST_VALUE("x") IGNORE NULLS OVER (ORDER BY "y" ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)')
  )
  expect_equal(
    dbplyr::translate_sql(nth(x, 2, order_by = y, na_rm = TRUE), con = s[["con"]]),
    dbplyr::sql('NTH_VALUE("x", 2) IGNORE NULLS OVER (ORDER BY "y")')
  )
})

with_locale(test.locale(), test_that)("paste() works", {
  s <- setup_mock_dplyr_connection()[["db"]]

  expect_equal(
    dbplyr::translate_sql(paste(a, b, c), con = s$con),
    dbplyr::sql("\"a\" || ' ' || \"b\" || ' ' || \"c\"")
  )

  expect_equal(
    dbplyr::translate_sql(paste(a, b, c, sep = "-"), con = s$con),
    dbplyr::sql("\"a\" || '-' || \"b\" || '-' || \"c\"")
  )

  expect_equal(
    dbplyr::translate_sql(paste0(a, b, c), con = s$con),
    dbplyr::sql("\"a\" || \"b\" || \"c\"")
  )
})
