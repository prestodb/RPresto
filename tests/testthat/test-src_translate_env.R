# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('src_translate_env')

source('utilities.R')

test_that('as() works', {
  v <- src_translate_env(setup_mock_dplyr_connection()[['db']])
  expect_equal(
    translate_sql(as(x, 0.0), variant=v),
    sql('CAST("x" AS "DOUBLE")')
  )

  expect_equal(
    translate_sql_q(list(substitute(as(x, l), list(l=list()))), variant=v),
    sql('CAST("x" AS "ARRAY<VARCHAR>")')
  )


  substituted.expression <- substitute(as(x, l), list(l=Sys.Date()))
  expect_equal(
    translate_sql_q(list(substituted.expression), variant=v),
    sql('CAST("x" AS "DATE")')
  )

  # Hacky dummy table so that we can test substitution
  s <- setup_live_dplyr_connection()[['db']]
  t <- tbl(s, from=sql('SELECT 1'), vars=list(as.name('a')))

  l <- list(a=1L)
  expect_equal(
    translate_sql(as(x, l), tbl=t),
    sql('CAST("x" AS "MAP<VARCHAR, BIGINT>")')
  )

  expect_equal(
    translate_sql(as(x, local(list(a=Sys.time()))), tbl=t),
    sql('CAST("x" AS "MAP<VARCHAR, TIMESTAMP>")')
  )

  r <- as.raw(0)
  p <- as.POSIXct('2001-02-03 04:05:06', tz='Europe/Istanbul')
  query <- as.character(
    (
      t
      %>% transmute(
        b=as(a, r),
        c=as(a, p),
        d=as(a, TRUE)
      )
    )[['query']][['sql']]
  )
  expect_more_than(
    length(grep(
      paste0(
        '^SELECT ',
        'CAST\\("a" AS "VARBINARY"\\) AS "b", ' ,
        'CAST\\("a" AS "TIMESTAMP WITH TIME ZONE"\\) AS "c", ',
        'CAST\\("a" AS "BOOLEAN"\\) AS "d"\n',
        'FROM \\(SELECT 1\\) AS "zzz\\d+"$'),
      query
      )
    ),
    0
  )
})
