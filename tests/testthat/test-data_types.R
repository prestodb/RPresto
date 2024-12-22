# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "data types"))

test_that("Queries return the correct primitive types", {
  conn <- setup_live_connection()

  expect_equal_data_frame(
    dbGetQuery(conn, "select true bool"),
    tibble::tibble(bool = TRUE)
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select 1 one"),
    tibble::tibble(one = 1)
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select cast(1 as double) one"),
    tibble::tibble(one = 1.0)
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select 'one' one"),
    tibble::tibble(one = "one")
  )
})

test_that("Queries return the correct array types", {
  conn <- setup_live_connection()
  expect_equal_data_frame(
    dbGetQuery(conn, "select array[] arr"),
    tibble::tibble(arr = list(integer(0)))
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select array[null] arr"),
    tibble::tibble(arr = list(NA_integer_))
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select array[1] arr"),
    tibble::tibble(arr = list(1L))
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select array['1'] arr"),
    tibble::tibble(arr = list("1"))
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select array[1, 2] arr"),
    tibble::tibble(arr = list(c(1L, 2L)))
  )
  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "
        select array[1, 2] arr
        union all
        select array[1]
        order by arr
      "
    ),
    tibble::tibble(arr = list(c(1L), c(1L, 2L)))
  )
})

test_that("Queries return the correct map types", {
  conn <- setup_live_connection()
  expect_equal_data_frame(
    dbGetQuery(conn, "select map(array[1], array[1]) m"),
    tibble::tibble(m = list(c(`1` = 1L)))
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select map(array['1'], array['1']) m"),
    tibble::tibble(m = list(c(`1` = "1")))
  )
  expect_equal_data_frame(
    dbGetQuery(conn, "select map(array[1, 2], array[3, 4]) m"),
    tibble::tibble(m = list(c(`1` = 3, `2` = 4)))
  )
  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "
        select map(array[1], array[2]) m, 1 r
        union all
        select map(array[3], array[4]) m, 2 r
        order by r
      "
    ),
    tibble::tibble(
      m = list(c(`1` = 2), c(`3` = 4)),
      r = c(1, 2)
    )
  )
})

test_that("Queries return the correct row types", {
  conn <- setup_live_connection()
  e <- tibble::tibble(r = list(list(x = 1)))
  expect_equal_data_frame(
    dbGetQuery(conn, "select cast(row(1) as row(x bigint)) r"),
    e
  )
  e$r <- list(list(x = 1, y = "a"))
  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "select cast(row(1, 'a') as row(x bigint, y varchar)) r"
    ),
    e
  )
  e <- tibble::tibble(r = list(list(x = 1L, y = "a"), list(x = 2L, y = "b")))
  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "
        select cast(row(1, 'a') as row(x bigint, y varchar)) r
        union all
        select cast(row(2, 'b') as row(x bigint, y varchar)) r
        order by r.x
      "
    ),
    e
  )
})

test_that("all data types work", {
  conn <- setup_live_connection(session.timezone = test.timezone())

  expect_equal_data_frame(
    dbGetQuery(conn, paste0("
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
        CAST('a' AS CHAR) AS type_char,
        CAST('a' AS VARBINARY) AS type_varbinary,
        JSON_PARSE('{\"a\": 1}') AS type_json,
        DATE '2015-03-01' AS type_date,
        TIME '01:02:03.456' AS type_time,
        TIME '01:02:03.456 ", ifelse(conn@use.trino.headers, tz_to_offset("UTC"), "UTC"), "' AS type_time_with_timezone,
        TIMESTAMP '2001-08-22 03:04:05.321' AS type_timestamp,
        TIMESTAMP '2001-08-22 03:04:05.321 UTC'
          AS type_timestamp_with_timezone,
        INTERVAL '13' MONTH AS type_interval_year_to_month,
        INTERVAL '626704.321' SECOND AS type_interval_day_to_second,
        ARRAY[1,2,3] AS type_array_bigint,
        MAP(ARRAY['a'], ARRAY[0]) AS type_map_varchar_bigint
    ")),
    tibble::tibble(
      type_boolean = TRUE,
      type_tinyint = 1L,
      type_smallint = 1L,
      type_integer = 1L,
      type_bigint = 1L,
      type_real = 1.0,
      type_double = 1.0,
      type_decimal = "1.414",
      type_varchar = "a",
      type_char = "a",
      type_varbinary = list(charToRaw("a")),
      type_json = '{"a":1}',
      type_date = as.Date("2015-03-01"),
      type_time = hms::as_hms("01:02:03.456"),
      type_time_with_timezone = hms::as_hms("06:47:03.456"),
      type_timestamp =
        lubridate::with_tz(
          as.POSIXct("2001-08-22 03:04:05.321", tz = conn@session.timezone),
          tz = test.timezone()
        ),
      type_timestamp_with_timezone = as.POSIXct("2001-08-22 08:49:05.321", tz = test.timezone()),
      type_interval_year_to_month = lubridate::duration(13, units = "months"),
      type_interval_day_to_second =
        lubridate::duration(626704.321, units = "seconds"),
      type_array_bigint = list(c(1L, 2L, 3L)),
      type_map_varchar_bigint = list(c(a = 0L))
    )
  )

  e <- tibble::tibble(
    type_boolean = NA,
    type_tinyint = NA_integer_,
    type_smallint = NA_integer_,
    type_integer = NA_integer_,
    type_bigint = NA_integer_,
    type_real = NA_real_,
    type_double = NA_real_,
    type_decimal = NA_character_,
    type_varchar = NA_character_,
    type_char = NA_character_,
    # raw type doesn't have an NA value
    type_varbinary = list(raw(0)),
    type_json = NA_character_,
    type_date = as.Date(NA),
    type_time = hms::as_hms(NA_character_),
    type_time_with_timezone = hms::as_hms(NA_character_),
    type_timestamp = as.POSIXct(NA_character_, tz = test.timezone()),
    type_timestamp_with_timezone =
      as.POSIXct(NA_character_, tz = test.timezone()),
    type_array_bigint = list(NA_integer_),
    type_map_varchar_bigint = list(NA_integer_)
  )
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
        CAST(NULL AS CHAR) AS type_char,
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
