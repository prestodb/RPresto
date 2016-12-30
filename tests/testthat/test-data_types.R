# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('data types')

source('utilities.R')

# helper functions
data_frame <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}

test_that("Queries return the correct primitive types", {
  conn <- setup_live_connection()

  expect_error(dbGetQuery(conn, "select null unknown"),
               'Unsupported column type',
               label='column of type "unknown"')
  expect_equal_data_frame(dbGetQuery(conn, "select true bool"),
               data_frame(bool = TRUE))
  expect_equal_data_frame(dbGetQuery(conn, "select 1 one"),
               data_frame(one = 1))
  expect_equal_data_frame(dbGetQuery(conn, "select 1.0 one"),
               data_frame(one = 1.0))
  expect_equal_data_frame(dbGetQuery(conn, "select 'one' one"),
               data_frame(one = 'one'))
})

test_that("Queries return the correct array types", {
  conn <- setup_live_connection()
  e <- data_frame(arr=NA)
  e[[1]] <- list(list())
  expect_equal_data_frame(dbGetQuery(conn, "select array[] arr"), e)
  e[[1]] <- list(list(NA))
  expect_equal_data_frame(dbGetQuery(conn, "select array[null] arr"), e)
  e[[1]] <- list(list(1))
  expect_equal_data_frame(dbGetQuery(conn, "select array[1] arr"), e)
  e[[1]] <- list(list('1'))
  expect_equal_data_frame(dbGetQuery(conn, "select array['1'] arr"), e)
  e[[1]] <- list(list(1, 2))
  expect_equal_data_frame(dbGetQuery(conn, "select array[1, 2] arr"), e)
  e <- data_frame(arr=rep(NA, 2))
  e[[1]] <- list(list(1), list(1, 2))
  expect_equal_data_frame(dbGetQuery(conn, "select array[1, 2] arr
                         union all
                         select array[1]
                         order by arr"),
               e)
})

test_that("Queries return the correct map types", {
  conn <- setup_live_connection()
  e <- data_frame(m=NA)
  e[[1]] <- list(list('1'=1))
  expect_equal_data_frame(dbGetQuery(conn, "select map(array[1], array[1]) m"), e)
  e[[1]] <- list(list('1'='1'))
  expect_equal_data_frame(dbGetQuery(conn, "select map(array['1'], array['1']) m"), e)
  e[[1]] <- list(list('1'=3, '2'=4))
  expect_equal_data_frame(
    dbGetQuery(conn, "select map(array[1, 2], array[3, 4]) m"),
    e
  )
  e <- data_frame(m=rep(NA, 2), r=c(1,2))
  e[[1]] <- list(list('1'=2), list('3'=4))
  expect_equal_data_frame(dbGetQuery(conn, "select map(array[1], array[2]) m, 1 r
                         union all
                         select map(array[3], array[4]) m, 2 r
                         order by r"),
               e)
})

test_that("all data types work", {
  conn <- setup_live_connection(session.timezone=test.timezone())
  e <- data_frame(
    type_boolean=TRUE,
    type_tinyint=1L,
    type_smallint=1L,
    type_integer=1L,
    type_bigint=1L,
    type_real=1.0,
    type_double=1.0,
    type_decimal='1.414',
    type_varchar='a',
    type_varbinary=NA,
    type_json='{"a":1}',
    type_date=as.Date('2015-03-01'),
    type_time='01:02:03.456',
    type_time_with_timezone='01:02:03.456 UTC',
    type_timestamp
      =as.POSIXct('2001-08-22 03:04:05.321', tz=test.timezone()),
    type_timestamp_with_timezone
      =as.POSIXct('2001-08-22 03:04:05.321', tz='UTC'),
    type_interval_year_to_month='1-1',
    type_interval_day_to_second='7 06:05:04.321',
    type_array_bigint=NA,
    type_map_varchar_bigint=NA
  )
  attr(e[['type_timestamp_with_timezone']], 'tzone') <- 'UTC'
  attr(e[['type_timestamp']], 'tzone') <- test.timezone()
  e[['type_varbinary']] <- list(charToRaw('a'))
  e[['type_array_bigint']] <- list(list(1, 2, 3))
  e[['type_map_varchar_bigint']] <- list(list(a=0))

  expect_equal_data_frame(
    dbGetQuery(conn, "
      SELECT
        true AS type_boolean,
        CAST(1 AS TINYINT) AS type_tinyint,
        CAST(1 AS SMALLINT) AS type_smallint,
        CAST(1 AS INTEGER) AS type_integer,
        CAST(1 AS BIGINT) AS type_bigint,
        CAST(1.0 AS REAL) AS type_real,
        CAST(1.0 AS DOUBLE) AS type_double,
        DECIMAL '1.414' AS type_decimal,
        CAST('a' AS VARCHAR) AS type_varchar,
        CAST('a' AS VARBINARY) AS type_varbinary,
        JSON_PARSE('{\"a\": 1}') AS type_json,
        DATE '2015-03-01' AS type_date,
        TIME '01:02:03.456' AS type_time,
        TIME '01:02:03.456 UTC' AS type_time_with_timezone,
        TIMESTAMP '2001-08-22 03:04:05.321' AS type_timestamp,
        TIMESTAMP '2001-08-22 03:04:05.321 UTC'
          AS type_timestamp_with_timezone,
        INTERVAL '13' MONTH AS type_interval_year_to_month,
        INTERVAL '626704.321' SECOND AS type_interval_day_to_second,
        ARRAY[1,2,3] AS type_array_bigint,
        MAP(ARRAY['a'], ARRAY[0]) AS type_map_varchar_bigint
    "),
    e
  )

  e <- data_frame(
    type_boolean=NA,
    type_tinyint=NA_integer_,
    type_smallint=NA_integer_,
    type_integer=NA_integer_,
    type_bigint=NA_integer_,
    type_real=NA_real_,
    type_double=NA_real_,
    type_decimal=NA_character_,
    type_varchar=NA_character_,
    type_varbinary=NA,
    type_json=NA_character_,
    type_date=as.Date(NA),
    type_time=NA_character_,
    type_time_with_timezone=NA_character_,
    type_timestamp=as.POSIXct(NA_character_),
    type_timestamp_with_timezone=as.POSIXct(NA_character_),
    type_array_bigint=NA,
    type_map_varchar_bigint=NA
  )
  attr(e[['type_timestamp_with_timezone']], 'tzone') <- NULL
  attr(e[['type_timestamp']], 'tzone') <- test.timezone()
  e[['type_varbinary']] <- list(NA)
  e[['type_array_bigint']] <- list(NA)
  e[['type_map_varchar_bigint']] <- list(NA)
  expect_equal_data_frame(
    dbGetQuery(conn, "
      SELECT
        CAST(NULL AS BOOLEAN) AS type_boolean,
        CAST(NULL AS TINYINT) AS type_tinyint,
        CAST(NULL AS SMALLINT) AS type_smallint,
        CAST(NULL AS INTEGER) AS type_integer,
        CAST(NULL AS BIGINT) AS type_bigint,
        CAST(NULL AS REAL) AS type_real,
        CAST(NULL AS DOUBLE) AS type_double,
        CAST(NULL AS DECIMAL) AS type_decimal,
        CAST(NULL AS VARCHAR) AS type_varchar,
        CAST(NULL AS VARBINARY) AS type_varbinary,
        JSON_PARSE(NULL) AS type_json,
        CAST(NULL AS DATE) AS type_date,
        CAST(NULL AS TIME) AS type_time,
        CAST(NULL AS TIME WITH TIME ZONE) AS type_time_with_timezone,
        CAST(NULL AS TIMESTAMP) AS type_timestamp,
        CAST(NULL AS TIMESTAMP WITH TIME ZONE)
          AS type_timestamp_with_timezone,
        CAST(NULL AS ARRAY<BIGINT>) AS type_array_bigint,
        CAST(NULL AS MAP<VARCHAR, BIGINT>) AS type_map_varchar_bigint
    "),
    e
  )
})
