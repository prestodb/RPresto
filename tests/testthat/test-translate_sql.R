# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('translate_sql')

source('utilities.R')

with_locale(test.locale(), test_that)('as() works', {
  if (!requireNamespace('dplyr', quietly=TRUE)) {
    skip('dplyr not available')
  }

  translate_sql <- RPresto:::dbplyr_compatible('translate_sql')
  translate_sql_ <- RPresto:::dbplyr_compatible('translate_sql_')

  s <- setup_mock_dplyr_connection()[['db']]

  expect_equal(
    translate_sql(as(x, 0.0), con=s[['con']]),
    dplyr::sql('CAST("x" AS DOUBLE)')
  )
  expect_equal(
    translate_sql(pmax(x), con=s[['con']]),
    dplyr::sql('GREATEST("x")')
  )
  expect_equal(
    translate_sql_(
      list(substitute(as(x, l), list(l=list()))),
      con=s[['con']]
    ),
    dplyr::sql('CAST("x" AS ARRAY<VARCHAR>)')
  )

  substituted.expression <- substitute(as(x, l), list(l=Sys.Date()))
  expect_equal(
    translate_sql_(list(substituted.expression), con=s[['con']]),
    dplyr::sql('CAST("x" AS DATE)')
  )

  # Hacky dummy table so that we can test substitution
  s <- setup_live_dplyr_connection()[['db']]
  t <- dplyr::tbl(
    s,
    from=dplyr::sql_subquery(
      s[['con']],
      dplyr::sql('SELECT 1')
    ),
    vars=c('x')
  )

  l <- list(a=1L)
  expect_equal(
    translate_sql(
      as(x, !!l),
      con=s[['con']]
    ),
    dplyr::sql('CAST("x" AS MAP<VARCHAR, BIGINT>)')
  )
  expect_equal(
    translate_sql(
      as(x, !!local(list(a=Sys.time()))),
      con=s[['con']]
    ),
    dplyr::sql('CAST("x" AS MAP<VARCHAR, TIMESTAMP>)')
  )
  r <- as.raw(0)
  p <- as.POSIXct('2001-02-03 04:05:06', tz='Europe/Istanbul')
  l <- TRUE
  sql_render <- RPresto:::dbplyr_compatible('sql_render')
  query <- as.character(sql_render(dplyr::transmute(
    t,
    b=as(a, r),
    c=as(a, p),
    d=as(a, l)
  )))
  expect_true(
    grepl(
      paste0(
        '^SELECT "b"( AS "b")?, "c"( AS "c")?, "d"( AS "d")?.*',
        'FROM \\(',
          'SELECT ',
            '"_col0", ',
            'CAST\\("a" AS VARBINARY\\) AS "b", ' ,
            'CAST\\("a" AS TIMESTAMP WITH TIME ZONE\\) AS "c", ',
            'CAST\\("a" AS BOOLEAN\\) AS "d"\n',
          'FROM \\(',
            '\\(SELECT 1\\) "[_0-9a-z]+"',
          '\\) "[_0-9a-z]+"',
        '\\) "[_0-9a-z]+"$'
      ),
      query
    )
  )
})

with_locale(test.locale(), test_that)('as.<type>() works', {
  if (!requireNamespace('dplyr', quietly=TRUE)) {
    skip('dplyr not available')
  }

  translate_sql <- RPresto:::dbplyr_compatible('translate_sql')
  s <- setup_mock_dplyr_connection()[['db']]
  expect_equal(
    translate_sql(as.character(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS VARCHAR)')
  )
  expect_equal(
    translate_sql(as.numeric(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS DOUBLE)')
  )
  expect_equal(
    translate_sql(as.double(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS DOUBLE)')
  )
  expect_equal(
    translate_sql(as.integer(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS BIGINT)')
  )
  expect_equal(
    translate_sql(as.Date(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS DATE)')
  )
  expect_equal(
    translate_sql(as.logical(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS BOOLEAN)')
  )
  expect_equal(
    translate_sql(as.raw(x), con=s[['con']]),
    dplyr::sql('CAST("x" AS VARBINARY)')
  )
})
