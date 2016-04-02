# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('src_translate_env')

source('utilities.R')

with_locale(test.locale(), test_that)('as() works', {

  conn <- setup_mock_dplyr_connection()[['db']]
  f <- try(getFromNamespace('src_translate_env', 'dplyr'), silent=TRUE)
  if (!inherits(f, 'try-error')) {
    v <- f(conn)
    expect_equal(
      dplyr::translate_sql(as(x, 0.0), variant=v),
      dplyr::sql('CAST("x" AS "DOUBLE")')
    )
    expect_equal(
      dplyr::translate_sql_q(
        list(substitute(as(x, l), list(l=list()))),
        variant=v
      ),
      dplyr::sql('CAST("x" AS "ARRAY<VARCHAR>")')
    )

    substituted.expression <- substitute(as(x, l), list(l=Sys.Date()))
    expect_equal(
      dplyr::translate_sql_q(list(substituted.expression), variant=v),
      dplyr::sql('CAST("x" AS "DATE")')
    )

    substituted.expression <- substitute(as(x, l), list(l=Sys.Date()))
    expect_equal(
      dplyr::translate_sql_q(list(substituted.expression), variant=v),
      dplyr::sql('CAST("x" AS "DATE")')
    )
  } else {
    expect_equal(
      dplyr::translate_sql(as(x, 0.0), con=conn[['con']]),
      dplyr::sql('CAST("x" AS "DOUBLE")')
    )
    expect_equal(
      dplyr::translate_sql(pmax(x), con=conn[['con']]),
      dplyr::sql('GREATEST("x")')
    )
    expect_equal(
      dplyr::translate_sql_(
        list(substitute(as(x, l), list(l=list()))),
        con=conn[['con']]
      ),
      dplyr::sql('CAST("x" AS "ARRAY<VARCHAR>")')
    )

    substituted.expression <- substitute(as(x, l), list(l=Sys.Date()))
    expect_equal(
      dplyr::translate_sql_(list(substituted.expression), con=conn[['con']]),
      dplyr::sql('CAST("x" AS "DATE")')
    )
  }

  # Hacky dummy table so that we can test substitution
  s <- setup_live_dplyr_connection()[['db']]
  t <- dplyr::tbl(
    s,
    from=dplyr::sql_subquery(
      s[['con']],
      dplyr::sql('SELECT 1')
    ),
    vars=list(as.name('x'))
  )

  f <- try(getFromNamespace('src_translate_env', 'dplyr'), silent=TRUE)
  if (!inherits(f, 'try-error')) {
    l <- list(a=1L)
    expect_equal(
      dplyr::translate_sql(as(x, l), tbl=t),
      dplyr::sql('CAST("x" AS "MAP<VARCHAR, BIGINT>")')
    )

    expect_equal(
      dplyr::translate_sql(as(x, local(list(a=Sys.time()))), tbl=t),
      dplyr::sql('CAST("x" AS "MAP<VARCHAR, TIMESTAMP>")')
    )

    r <- as.raw(0)
    p <- as.POSIXct('2001-02-03 04:05:06', tz='Europe/Istanbul')
    query <- as.character(
      (
        dplyr::transmute(
          t,
          b=as(a, r),
          c=as(a, p),
          d=as(a, TRUE)
        )
      )[['query']][['sql']]
    )
    expect_true(
      grepl(
        paste0(
          '^SELECT ',
          'CAST\\("a" AS "VARBINARY"\\) AS "b", ' ,
          'CAST\\("a" AS "TIMESTAMP WITH TIME ZONE"\\) AS "c", ',
          'CAST\\("a" AS "BOOLEAN"\\) AS "d"\n',
          'FROM \\(\\(SELECT 1\\) AS "(_W|zzz)\\d+"\\) AS "[_0-9a-z]+"$'
        ),
        query
      )
    )
  } else {
    l <- list(a=1L)
    expect_equal(
      dplyr::translate_sql(as(x, l), con=s[['con']], vars='x'),
      dplyr::sql('CAST("x" AS "MAP<VARCHAR, BIGINT>")')
    )

    expect_equal(
      dplyr::translate_sql(
        as(x, local(list(a=Sys.time()))),
        vars='x',
        con=s[['con']]
      ),
      dplyr::sql('CAST("x" AS "MAP<VARCHAR, TIMESTAMP>")')
    )

    r <- as.raw(0)
    p <- as.POSIXct('2001-02-03 04:05:06', tz='Europe/Istanbul')
    query <- as.character(dplyr::sql_render(dplyr::transmute(
      t,
      b=as(a, r),
      c=as(a, p),
      d=as(a, TRUE)
    )))

    expect_true(
      grepl(
        paste0(
          '^SELECT "b" AS "b", "c" AS "c", "d" AS "d".*',
          'FROM \\(',
            'SELECT ',
              '"_col0", ',
              'CAST\\("a" AS "VARBINARY"\\) AS "b", ' ,
              'CAST\\("a" AS "TIMESTAMP WITH TIME ZONE"\\) AS "c", ',
              'CAST\\("a" AS "BOOLEAN"\\) AS "d"\n',
            'FROM \\(',
              '\\(SELECT 1\\) "[_0-9a-z]+"',
            '\\) "[_0-9a-z]+"',
          '\\) "[_0-9a-z]+"$'
        ),
        query
      )
    )
  }
})
