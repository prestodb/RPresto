# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("presto_field")

source("utilities.R")

# bool type
# tinyint type
# smallint type
# integer type
# real type
# double type
# character type
# char type
# bytes type
# date type
# timestamp type
# timestamp with timezone type
# time type
# time with timezone type
# interval (year to month) type
# interval (day to second) type
.test_primitive_types <- function(
  conn, timezone = "America/Los_Angeles", type = "Presto"
) {
  # bool type
  expect_equal_data_frame(
    df.boolean <- dbGetQuery(conn, "select true as type_bool"),
    tibble::tibble(type_bool = TRUE)
  )
  expect_type(df.boolean$type_bool, "logical")
  # tinyint type
  expect_equal_data_frame(
    df.tinyint <- dbGetQuery(conn, "select cast(1 as tinyint) as type_tinyint"),
    tibble::tibble(type_tinyint = 1)
  )
  expect_type(df.tinyint$type_tinyint, "integer")
  # smallint type
  expect_equal_data_frame(
    df.smallint <- dbGetQuery(
      conn, "select cast(1 as smallint) as type_smallint"
    ),
    tibble::tibble(type_smallint = 1)
  )
  expect_type(df.smallint$type_smallint, "integer")
  # integer type
  expect_equal_data_frame(
    df.integer <- dbGetQuery(
      conn, "select cast(1 as integer) as type_integer"
    ),
    tibble::tibble(type_integer = 1)
  )
  expect_type(df.integer$type_integer, "integer")
  #  real type
  expect_equal_data_frame(
    df.real <- dbGetQuery(conn, "select cast(3.14 as real) as type_real"),
    tibble::tibble(type_real = 3.14)
  )
  expect_type(df.real$type_real, "double")
  # double type
  expect_equal_data_frame(
    df.double <- dbGetQuery(conn, "select cast(3.14 as double) as type_double"),
    tibble::tibble(type_double = 3.14)
  )
  expect_type(df.double$type_double, "double")
  # decimal type
  expect_equal_data_frame(
    df.decimal <- dbGetQuery(
      conn, "select cast(9007199254740991.5 as decimal(17,1)) as type_decimal"
    ),
    tibble::tibble(type_decimal = "9007199254740991.5")
  )
  expect_type(df.decimal$type_decimal, "character")
  # character type
  expect_equal_data_frame(
    df.character <- dbGetQuery(conn, "select 'one' as type_character"),
    tibble::tibble(type_character = "one")
  )
  expect_type(df.character$type_character, "character")
  # char type
  expect_equal_data_frame(
    df.char <- dbGetQuery(conn, "select cast('a' as char) as type_char"),
    tibble::tibble(type_char = "a")
  )
  expect_type(df.char$type_char, "character")
  # bytes type
  expect_equal_data_frame(
    df.bytes <- dbGetQuery(
      conn,
      "select cast('abc' as varbinary) as type_bytes"
    ),
    tibble::tibble(type_bytes = list(charToRaw("abc")))
  )
  expect_type(df.bytes$type_bytes, "list")
  expect_type(df.bytes$type_bytes[[1]], "raw")
  # date type
  expect_equal_data_frame(
    df.date <- dbGetQuery(conn, "select date '2000-01-01' as type_date"),
    tibble::tibble(type_date = as.Date("2000-01-01"))
  )
  expect_s3_class(df.date$type_date, "Date")
  expect_null(attr(df.date$type_date[[1]], "tz"))
  # timestamp type
  expect_equal_data_frame(
    df.timestamp <- dbGetQuery(
      conn,
      "select timestamp '2000-01-01 01:02:03' as type_timestamp"
    ),
    tibble::tibble(
      type_timestamp = lubridate::with_tz(
        as.POSIXct("2000-01-01 01:02:03", tz = test.timezone()),
        tz = test.timezone()
      )
    )
  )
  expect_s3_class(df.timestamp$type_timestamp, "POSIXct")
  expect_equal(attr(df.timestamp$type_timestamp[[1]], "tz"), test.timezone())
  # timestamp with timezone type
  timestamp_date <- "2022-09-21"
  expect_equal_data_frame(
    df.timestamp_with_tz <- dbGetQuery(
      conn,
      paste0(
        "select timestamp '", timestamp_date, " 01:02:03 ",
        ifelse(
          type == "Presto",
          timezone,
          tz_to_offset(timezone, dt = as.Date(timestamp_date))
        ),
        "' as type_timestamp_with_tz"
      )
    ),
    tibble::tibble(
      type_timestamp_with_tz =
        lubridate::with_tz(
          as.POSIXct(paste0(timestamp_date, " 01:02:03"), tz = timezone),
          test.timezone()
        )
    )
  )
  expect_s3_class(df.timestamp_with_tz$type_timestamp_with_tz, "POSIXct")
  expect_equal(
    attr(df.timestamp_with_tz$type_timestamp_with_tz[[1]], "tz"),
    test.timezone()
  )
  # time type
  expect_equal_data_frame(
    df.time <- dbGetQuery(
      conn,
      "select time '01:02:03' as type_time"
    ),
    tibble::tibble(type_time = hms::as_hms("01:02:03"))
  )
  expect_s3_class(df.time$type_time, "difftime")
  expect_null(attr(df.time$type_time[[1]], "tz"))
  # time with timezone type
  expect_equal_data_frame(
    df.time_with_tz <- dbGetQuery(
      conn,
      paste0(
        "select time '01:02:03 ",
        ifelse(type == "Presto", timezone, tz_to_offset(timezone)),
        "' as type_time_with_tz"
      )
    ),
    tibble::tibble(
      type_time_with_tz = hms::as_hms(
        lubridate::with_tz(
          as.POSIXct(
            paste(Sys.Date(), "01:02:03", sep = " "),
            tz = timezone
          ),
          tz = test.timezone()
        )
      )
    )
  )
  expect_s3_class(df.time_with_tz$type_time_with_tz, "difftime")
  expect_null(attr(df.time_with_tz$type_time_with_tz[[1]], "tz"))
  # interval (year to month) type
  expect_equal_data_frame(
    df.interval_year_to_month <- dbGetQuery(
      conn,
      "select interval '14' month as type_interval_year_to_month"
    ),
    tibble::tibble(
      type_interval_year_to_month = lubridate::duration(14, units = "months")
    )
  )
  expect_s4_class(
    df.interval_year_to_month$type_interval_year_to_month,
    "Duration"
  )
  # interval (day to second) type
  expect_equal_data_frame(
    df.interval_day_to_second <- dbGetQuery(
      conn,
      paste(
        "select interval '2 4:5:6.500' day to second as",
        "type_interval_day_to_second"
      )
    ),
    tibble::tibble(
      type_interval_day_to_second =
        lubridate::duration(
          2 * 24 * 60 * 60 + 4 * 60 * 60 + 5 * 60 + 6 + 500 / 1000,
          units = "seconds"
        )
    )
  )
  expect_s4_class(
    df.interval_day_to_second$type_interval_day_to_second,
    "Duration"
  )
}

test_that("Queries return the correct primitive types", {
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Presto"
  )
  .test_primitive_types(conn.presto, type = "Presto")
  conn.trino <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Trino"
  )
  .test_primitive_types(conn.trino, type = "Trino")
})

.test_primitive_arrays <- function(conn, timezone = "America/Los_Angeles", type = "Presto") {
  # bool type
  expect_equal_data_frame(
    df.boolean <- dbGetQuery(conn, "select array[true, false] as type_bool"),
    tibble::tibble(type_bool = list(c(TRUE, FALSE)))
  )
  purrr::walk(df.boolean$type_bool, expect_type, "logical")
  # tinyint type
  expect_equal_data_frame(
    df.tinyint <- dbGetQuery(
      conn,
      "select array[cast(1 as tinyint), cast(2 as tinyint)] as type_tinyint"
    ),
    tibble::tibble(type_tinyint = list(c(1, 2)))
  )
  purrr::walk(df.tinyint$type_tinyint, expect_type, "integer")
  # smallint type
  expect_equal_data_frame(
    df.smallint <- dbGetQuery(
      conn,
      "select array[cast(1 as smallint), cast(2 as smallint)] as type_smallint"
    ),
    tibble::tibble(type_smallint = list(c(1, 2)))
  )
  purrr::walk(df.smallint$type_smallint, expect_type, "integer")
  # integer type
  expect_equal_data_frame(
    df.integer <- dbGetQuery(
      conn,
      "select array[cast(1 as integer), cast(2 as integer)] as type_integer"
    ),
    tibble::tibble(type_integer = list(c(1, 2)))
  )
  purrr::walk(df.integer$type_integer, expect_type, "integer")
  #  real type
  expect_equal_data_frame(
    df.real <- dbGetQuery(
      conn,
      "select array[cast(3.14 as real), cast(6.28 as real)] as type_real"
    ),
    tibble::tibble(type_real = list(c(3.14, 6.28)))
  )
  purrr::walk(df.real$type_real, expect_type, "double")
  # double type
  expect_equal_data_frame(
    df.double <- dbGetQuery(
      conn,
      "select array[cast(3.14 as double), cast(6.28 as double)] as type_double"
    ),
    tibble::tibble(type_double = list(c(3.14, 6.28)))
  )
  purrr::walk(df.double$type_double, expect_type, "double")
  # decimal type
  expect_equal_data_frame(
    df.decimal <- dbGetQuery(
      conn,
      paste0(
        "select array[cast(-9007199254740991.5 as decimal(17,1)), ",
        "cast(9007199254740991.5 as decimal(17,1))] as type_decimal"
      )
    ),
    tibble::tibble(
      type_decimal = list(c("-9007199254740991.5", "9007199254740991.5"))
    )
  )
  purrr::walk(df.decimal$type_decimal, expect_type, "character")
  # character type
  expect_equal_data_frame(
    df.character <- dbGetQuery(
      conn,
      "select array['one', 'two'] as type_character"
    ),
    tibble::tibble(type_character = list(c("one", "two")))
  )
  purrr::walk(df.character$type_character, expect_type, "character")
  # char type
  expect_equal_data_frame(
    df.char <- dbGetQuery(
      conn,
      "select array[cast('a' as char), cast('b' as char)] as type_char"
    ),
    tibble::tibble(type_char = list(c("a", "b")))
  )
  purrr::walk(df.char$type_char, expect_type, "character")
  # bytes type
  expect_equal_data_frame(
    df.bytes <- dbGetQuery(
      conn,
      "
      select
        array[
          cast('abc' as varbinary),
          cast('def' as varbinary)
        ] as type_bytes
      "
    ),
    tibble::tibble(type_bytes = list(list(charToRaw("abc"), charToRaw("def"))))
  )
  purrr::walk(df.bytes$type_bytes, expect_type, "list")
  purrr::walk(df.bytes$type_bytes[[1]], expect_type, "raw")
  # date type
  expect_equal_data_frame(
    df.date <- dbGetQuery(
      conn,
      "select array[date '2000-01-01', date '2000-01-02'] as type_date"
    ),
    tibble::tibble(
      type_date = list(c(as.Date("2000-01-01"), as.Date("2000-01-02")))
    )
  )
  purrr::walk(df.date$type_date, expect_s3_class, "Date")
  purrr::walk(df.date$type_date[[1]], ~ expect_null(attr(., "tz")))
  # timestamp type
  expect_equal_data_frame(
    df.timestamp <- dbGetQuery(
      conn,
      "
      select
        array[
          timestamp '2000-01-01 01:02:03',
          timestamp '2000-01-02 01:02:03'
        ] as type_timestamp
      "
    ),
    tibble::tibble(
      type_timestamp = list(
        lubridate::with_tz(
          as.POSIXct(
            c(
              "2000-01-01 01:02:03",
              "2000-01-02 01:02:03"
            ),
            tz = test.timezone()
          ),
          tz = test.timezone()
        )
      )
    )
  )
  purrr::walk(df.timestamp$type_timestamp, expect_s3_class, "POSIXct")
  purrr::walk(
    df.timestamp$type_timestamp[[1]],
    ~ expect_equal(attr(., "tz"), test.timezone())
  )
  # timestamp with timezone type
  timestamp_date <- "2022-09-21"
  expect_equal_data_frame(
    df.timestamp_with_tz <- dbGetQuery(
      conn,
      paste0("
      select
        array[
          timestamp '", timestamp_date, " 01:02:03 ",
          ifelse(
            type == "Presto",
            timezone,
            tz_to_offset(timezone, dt = as.Date(timestamp_date))
          ), "',
          timestamp '", timestamp_date, " 01:02:03 ",
          ifelse(
            type == "Presto",
            timezone,
            tz_to_offset(timezone, dt = as.Date(timestamp_date))
          ), "'
        ] as type_timestamp_with_tz
      ")
    ),
    tibble::tibble(
      type_timestamp_with_tz = list(
        lubridate::with_tz(
          as.POSIXct(
            c(
              paste0(timestamp_date, " 01:02:03"),
              paste0(timestamp_date, " 01:02:03")
            ),
            tz = timezone
          ),
          tz = test.timezone()
        )
      )
    )
  )
  purrr::walk(
    df.timestamp_with_tz$type_timestamp_with_tz,
    expect_s3_class, "POSIXct"
  )
  purrr::walk(
    df.timestamp_with_tz$type_timestamp_with_tz[[1]],
    ~ expect_equal(attr(., "tz"), test.timezone())
  )
  # time type
  expect_equal_data_frame(
    df.time <- dbGetQuery(
      conn,
      "select array[time '01:02:03', time '04:05:06'] as type_time"
    ),
    tibble::tibble(
      type_time = list(c(hms::as_hms("01:02:03"), hms::as_hms("04:05:06")))
    )
  )
  purrr::walk(df.time$type_time, expect_s3_class, "difftime")
  purrr::walk(df.time$type_time[[1]], ~ expect_null(attr(., "tz")))
  # time with timezone type
  expect_equal_data_frame(
    df.time_with_tz <- dbGetQuery(
      conn,
      paste0("
      select
        array[
          time '01:02:03 ", ifelse(type == "Presto", timezone, tz_to_offset(timezone)), "',
          time '04:05:06 ", ifelse(type == "Presto", timezone, tz_to_offset(timezone)), "'
        ] as type_time_with_tz
      ")
    ),
    tibble::tibble(
      type_time_with_tz = list(
        hms::as_hms(
          lubridate::with_tz(
            as.POSIXct(
              c(
                paste(Sys.Date(), "01:02:03", sep = ""),
                paste(Sys.Date(), "04:05:06", sep = "")
              ),
              tz = timezone
            ),
            tz = test.timezone()
          )
        )
      )
    )
  )
  purrr::walk(
    df.time_with_tz$type_time_with_tz,
    expect_s3_class, "difftime"
  )
  purrr::walk(
    df.time_with_tz$type_time_with_tz[[1]], ~ expect_null(attr(., "tz"))
  )
  # interval (year to month) type
  expect_equal_data_frame(
    df.interval_year_to_month <- dbGetQuery(
      conn,
      "
      select
        array[interval '14' month, interval '28' month]
          as type_interval_year_to_month
      "
    ),
    tibble::tibble(
      type_interval_year_to_month = list(
        c(
          lubridate::duration(14, units = "months"),
          lubridate::duration(28, units = "months")
        )
      )
    )
  )
  purrr::walk(
    df.interval_year_to_month$type_interval_year_to_month,
    expect_s4_class, "Duration"
  )
  # interval (day to second) type
  expect_equal_data_frame(
    df.interval_day_to_second <- dbGetQuery(
      conn,
      "
      select
        array[
          interval '2 4:5:6.500' day to second,
          interval '4 4:5:6.500' day to second
        ] as type_interval_day_to_second
      "
    ),
    tibble::tibble(
      type_interval_day_to_second = list(
        c(
          lubridate::duration(
            2 * 24 * 60 * 60 + 4 * 60 * 60 + 5 * 60 + 6 + 500 / 1000,
            units = "seconds"
          ),
          lubridate::duration(
            4 * 24 * 60 * 60 + 4 * 60 * 60 + 5 * 60 + 6 + 500 / 1000,
            units = "seconds"
          )
        )
      )
    )
  )
  purrr::walk(
    df.interval_day_to_second$type_interval_day_to_second,
    expect_s4_class, "Duration"
  )
}

test_that("Queries return the correct primitive types in arrays", {
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Presto"
  )
  .test_primitive_arrays(conn.presto, type = "Presto")
  conn.trino <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Trino"
  )
  .test_primitive_arrays(conn.trino, type = "Trino")
})

.test_maps <- function(conn) {
  # single map value -> 1-element named typed vector
  expect_equal_data_frame(
    df.map_single_value <- dbGetQuery(
      conn,
      "select map(array[1], array[date '2022-01-01']) as type_map_single_value"
    ),
    tibble::tibble(
      type_map_single_value = list(c(`1` = lubridate::ymd("2022-01-01")))
    )
  )
  purrr::walk(
    df.map_single_value$type_map_single_value, expect_s3_class, "Date"
  )
  purrr::walk(
    df.map_single_value$type_map_single_value,
    ~ expect_true(!is.null(names(.)))
  )
  purrr::walk(
    df.map_single_value$type_map_single_value,
    ~ expect_equal(length(.), 1)
  )
  # multiple map values -> multiple-element named typed vector
  expect_equal_data_frame(
    df.map_multiple_values <- dbGetQuery(
      conn,
      "
      select
        map(
          array[1, 2],
          array[date '2022-01-01', date '2022-01-02']
        ) as type_map_multiple_values
      "
    ),
    tibble::tibble(
      type_map_multiple_values = list(
        c(
          `1` = lubridate::ymd("2022-01-01"),
          `2` = lubridate::ymd("2022-01-02")
        )
      )
    )
  )
  purrr::walk(
    df.map_multiple_values$type_map_multiple_values, expect_s3_class, "Date"
  )
  purrr::walk(
    df.map_multiple_values$type_map_multiple_values,
    ~ expect_true(!is.null(names(.)))
  )
  purrr::walk(
    df.map_multiple_values$type_map_multiple_values,
    ~ expect_equal(length(.), 2)
  )
  # nested single map value -> 1-element named list
  expect_equal_data_frame(
    df.map_nested_value <- dbGetQuery(
      conn,
      "
      select
        map(array[1], array[map(array['x'], array[date '2022-01-01'])])
          as type_map_nested_value
      "
    ),
    tibble::tibble(
      type_map_nested_value = list(
        list(`1` = c(x = lubridate::ymd("2022-01-01")))
      )
    )
  )
  purrr::walk(
    df.map_nested_value$type_map_nested_value, expect_is, "list"
  )
  purrr::walk(
    df.map_nested_value$type_map_nested_value,
    ~ expect_true(!is.null(names(.)))
  )
  purrr::walk(
    df.map_nested_value$type_map_nested_value,
    ~ expect_equal(length(.), 1)
  )
  purrr::walk(
    df.map_nested_value$type_map_nested_value,
    ~ purrr::walk(., expect_s3_class, "Date")
  )
  purrr::walk(
    df.map_nested_value$type_map_nested_value,
    ~ purrr::walk(., ~ expect_equal(length(.), 1))
  )
  # nested multiple map values -> multiple-element named list
  expect_equal_data_frame(
    df.map_nested_values <- dbGetQuery(
      conn,
      "
      select
        map(
          array[1, 2],
          array[
            map(array['x', 'y'], array[date '2022-01-01', date '2022-01-02']),
            map(array['x', 'y'], array[date '2022-01-03', date '2022-01-04'])
          ]
        ) as type_map_nested_values
      "
    ),
    tibble::tibble(
      type_map_nested_values = list(
        list(
          `1` = c(
            x = lubridate::ymd("2022-01-01"), y = lubridate::ymd("2022-01-02")
          ),
          `2` = c(
            x = lubridate::ymd("2022-01-03"), y = lubridate::ymd("2022-01-04")
          )
        )
      )
    )
  )
  purrr::walk(
    df.map_nested_values$type_map_nested_values, expect_is, "list"
  )
  purrr::walk(
    df.map_nested_values$type_map_nested_values,
    ~ expect_true(!is.null(names(.)))
  )
  purrr::walk(
    df.map_nested_values$type_map_nested_values,
    ~ expect_equal(length(.), 2)
  )
  purrr::walk(
    df.map_nested_values$type_map_nested_values,
    ~ purrr::walk(., expect_s3_class, "Date")
  )
  purrr::walk(
    df.map_nested_values$type_map_nested_values,
    ~ purrr::walk(., ~ expect_equal(length(.), 2))
  )
  # array of single-value maps -> unnamed list of 1-element named typed vector
  expect_equal_data_frame(
    df.array_of_map_single_value <- dbGetQuery(
      conn,
      "
      select
        array[
          map(array[1], array[date '2022-01-01']),
          map(array[2], array[date '2022-01-02'])
        ] as type_map_single_value
      "
    ),
    tibble::tibble(
      type_map_single_value = list(
        list(
          c(`1` = lubridate::ymd("2022-01-01")),
          c(`2` = lubridate::ymd("2022-01-02"))
        )
      )
    )
  )
  purrr::walk(
    df.array_of_map_single_value$type_map_single_value, expect_is, "list"
  )
  purrr::walk(
    df.array_of_map_single_value$type_map_single_value,
    ~ expect_true(is.null(names(.)))
  )
  purrr::walk(
    df.array_of_map_single_value$type_map_single_value,
    ~ expect_equal(length(.), 2)
  )
  purrr::walk(
    df.array_of_map_single_value$type_map_single_value,
    ~ purrr::walk(., expect_s3_class, "Date")
  )
  purrr::walk(
    df.array_of_map_single_value$type_map_single_value,
    ~ purrr::walk(., ~ expect_equal(length(.), 1))
  )
  # array of multiple-element maps -> unnamed list of multiple-element named
  # typed vector
  expect_equal_data_frame(
    df.array_of_map_multiple_values <- dbGetQuery(
      conn,
      "
      select
        array[
          map(array[1, 2], array[date '2022-01-01', date '2022-01-02']),
          map(array[1, 2], array[date '2022-01-03', date '2022-01-04'])
        ] as type_map_multiple_values
      "
    ),
    tibble::tibble(
      type_map_multiple_values = list(
        list(
          c(
            `1` = lubridate::ymd("2022-01-01"),
            `2` = lubridate::ymd("2022-01-02")
          ),
          c(
            `1` = lubridate::ymd("2022-01-03"),
            `2` = lubridate::ymd("2022-01-04")
          )
        )
      )
    )
  )
  purrr::walk(
    df.array_of_map_multiple_values$type_map_multiple_values, expect_is, "list"
  )
  purrr::walk(
    df.array_of_map_multiple_values$type_map_multiple_values,
    ~ expect_true(is.null(names(.)))
  )
  purrr::walk(
    df.array_of_map_multiple_values$type_map_multiple_values,
    ~ expect_equal(length(.), 2)
  )
  purrr::walk(
    df.array_of_map_multiple_values$type_map_multiple_values,
    ~ purrr::walk(., expect_s3_class, "Date")
  )
  purrr::walk(
    df.array_of_map_multiple_values$type_map_multiple_values,
    ~ purrr::walk(., ~ expect_equal(length(.), 2))
  )
}

test_that("Queries return the correct map types", {
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Presto"
  )
  .test_maps(conn.presto)
  conn.trino <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Trino"
  )
  .test_maps(conn.trino)
})

.test_rows <- function(conn) {
  # single row value is mapped to a single-value named list
  expect_equal_data_frame(
    df.row_single_value <- dbGetQuery(
      conn, "select cast(row(1) as row(x int)) as type_row_single_value"
    ),
    tibble::tibble(type_row_single_value = list(list(x = 1)))
  )
  # single null row value is mapped to a single NA
  expect_equal_data_frame(
    df.row_single_null <- dbGetQuery(
      conn, "select cast(null as row(x int)) as type_row_single_null"
    ),
    tibble::tibble(type_row_single_null = list(NA))
  )
  # single null row value with single row value is mapped to a list consisting
  # of a NA and a named list
  expect_equal_data_frame(
    df.row_single_null_and_value <- dplyr::select(
      dplyr::arrange(
        dbGetQuery(
          conn,
          "
            select 0 as row, cast(null as row(x int)) as type_row_single_null_and_value
            union all
            select 1 as row, cast(row(1) as row(x int)) as type_row_single_null_and_value
          "
        ),
        row
      ),
      -row
    ),
    tibble::tibble(type_row_single_null_and_value = list(NA, list(x = 1)))
  )
  # multiple row values are mapped to a multiple-value named list
  expect_equal_data_frame(
    df.row_multiple_values <- dbGetQuery(
      conn,
      "
      select
        cast(row(1, 'a') as row(x int, y varchar)) as type_row_multiple_values
      "
    ),
    tibble::tibble(type_row_multiple_values = list(list(x = 1, y = "a")))
  )
  # an array of row values is mapped to a tibble
  expect_equal_data_frame(
    df.row_array_values <- dbGetQuery(
      conn,
      "
      select
        array[
          cast(row(1, 'a') as row(x int, y varchar)),
          cast(row(2, 'b') as row(x int, y varchar))
        ] as type_row_array_values
      "
    ),
    tibble::tibble(
      type_row_array_values = list(tibble::tibble(x = c(1, 2), y = c("a", "b")))
    )
  )
}

test_that("Queries return the correct row types", {
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Presto"
  )
  .test_rows(conn.presto)
  conn.trino <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Trino"
  )
  .test_rows(conn.trino)
})

.test_map_of_rows <- function(conn) {
  # map of single row is a named list
  expect_equal_data_frame(
    df.map_single_row <- dbGetQuery(
      conn,
      "
      select
        map(array[1], array[cast(row(1) as row(x int))]) as type_map_single_row
      "
    ),
    tibble::tibble(type_map_single_row = list(list(x = 1)))
  )
  # map of multiple rows is a tibble
  expect_equal_data_frame(
    df.map_multiple_rows <- dbGetQuery(
      conn,
      "
      select
        map(
          array[1, 2],
          array[
            cast(row(1) as row(x int)),
            cast(row(2) as row(x int))
          ]
        ) as type_map_multiple_rows
      "
    ),
    tibble::tibble(
      type_map_multiple_rows = list(tibble::tibble(x = c(1L, 2L)))
    )
  )
}

test_that("Queries return the correct map of row types", {
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Presto"
  )
  .test_map_of_rows(conn.presto)
  conn.trino <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Trino"
  )
  .test_map_of_rows(conn.trino)
})

.test_bigint <- function(conn) {
  expect_equal_data_frame(
    df.bigint <- dbGetQuery(
      conn, "select cast(1 as bigint) as type_bigint"
    ),
    tibble::tibble(type_bigint = 1L)
  )
  expect_type(df.bigint$type_bigint, "integer")

  expect_warning(
    df.bigint_overflow.1 <- dbGetQuery(
      conn,
      "select cast(pow(2, 60) as bigint) as type_bigint_overflow"
    ),
    "NAs produced by integer overflow"
  )
  expect_type(df.bigint_overflow.1$type_bigint_overflow, "integer")
  expect_equal_data_frame(
    df.bigint_overflow.1,
    tibble::tibble(type_bigint_overflow = NA_integer_)
  )

  expect_equal_data_frame(
    df.bigint_overflow.2 <- dbGetQuery(
      conn,
      "select cast(pow(2, 60) as bigint) as type_bigint_overflow",
      bigint = "integer64"
    ),
    tibble::tibble(type_bigint_overflow = bit64::as.integer64(2^60))
  )
  expect_s3_class(df.bigint_overflow.2$type_bigint_overflow, "integer64")

  expect_warning(
    df.bigint_overflow.3 <- dbGetQuery(
      conn,
      "select cast(pow(2, 60) as bigint) as type_bigint_overflow",
      bigint = "numeric"
    ),
    "integer precision lost while converting to double"
  )
  expect_type(df.bigint_overflow.3$type_bigint_overflow, "double")
  expect_equal_data_frame(
    df.bigint_overflow.3,
    tibble::tibble(type_bigint_overflow = as.double(2^60))
  )

  expect_equal_data_frame(
    df.bigint_overflow.4 <- dbGetQuery(
      conn,
      "select cast(pow(2, 60) as bigint) as type_bigint_overflow",
      bigint = "character"
    ),
    tibble::tibble(type_bigint_overflow = as.character(2^60))
  )
  expect_type(df.bigint_overflow.4$type_bigint_overflow, "character")
}

test_that("Queries return the correct handling of BIGINT", {
  conn.presto <- setup_live_connection(type = "Presto")
  .test_bigint(conn.presto)
  conn.trino <- setup_live_connection(type = "Trino")
  .test_bigint(conn.trino)
})

.test_infinity_nan <- function(conn) {
  expect_equal_data_frame(
    df.nan <- dbGetQuery(
      conn,
      "
        select infinity() as type_nan
        union all
        select -infinity() as type_nan
        union all
        select nan() as type_nan
        order by type_nan
      "
    ),
    tibble::tibble(type_nan = c(-Inf, Inf, NaN))
  )
}

test_that("Infinities and NaNs are handled properly", {
  conn.presto <- setup_live_connection(type = "Presto")
  .test_infinity_nan(conn.presto)
  conn.trino <- setup_live_connection(type = "Trino")
  .test_infinity_nan(conn.trino)
})

.test_null <- function(conn) {
  expect_equal_data_frame(
    dbGetQuery(
      conn,
      "
      select cast(null as varchar) as type_has_null
      union all
      select cast('abc' as varchar) as type_has_null
      order by type_has_null
    "
    ),
    tibble::tibble(type_has_null = c("abc", NA_character_))
  )
}

test_that("NULL values are properly handled", {
  conn.presto <- setup_live_connection(type = "Presto")
  .test_null(conn.presto)
  conn.trino <- setup_live_connection(type = "Trino")
  .test_null(conn.trino)
})

.test_empty <- function(conn) {
  expect_equal_data_frame(
    dbGetQuery(conn, "select * from (values(1)) as t(one) where 1 = 0"),
    tibble::tibble(one = integer(0))
  )
}

test_that("Empty output can be returned", {
  conn.presto <- setup_live_connection(type = "Presto")
  .test_empty(conn.presto)
  conn.trino <- setup_live_connection(type = "Trino")
  .test_empty(conn.trino)
})

.test_time_types <- function(
  conn, timezone = "America/Los_Angeles", type = "Presto"
) {
  timestamp_date <- "2022-09-21"
  timestamp_time <- "01:02:03.456"
  timestamp_string <- paste(timestamp_date, timestamp_time, sep = " ")
  # timestamp without explicit timezone
  expect_equal_data_frame(
    df.timestamp <- dbGetQuery(
      conn,
      paste0(
        "select type_timestamp, ",
        "timezone_hour(type_timestamp) as type_timestamp_offset_hr, ",
        "timezone_minute(type_timestamp) as type_timestamp_offset_min ",
        "from (select timestamp '", timestamp_string, "' as type_timestamp)"
      )
    ),
    tibble::tibble(
      type_timestamp = lubridate::with_tz(
        as.POSIXct(timestamp_string, tz = test.timezone()),
        tz = test.output.timezone()
      ),
      # session.timezone is used to interpret the timestamp
      type_timestamp_offset_hr =
        tz_to_offset_hr(test.timezone(), timestamp_date),
      type_timestamp_offset_min =
        tz_to_offset_min(test.timezone(), timestamp_date)
    )
  )
  expect_s3_class(df.timestamp$type_timestamp, "POSIXct")
  expect_equal(
    attr(df.timestamp$type_timestamp[[1]], "tz"), test.output.timezone()
  )
  # timestamp with timezone
  expect_equal_data_frame(
    df.timestamp_with_tz <- dbGetQuery(
      conn,
      paste0(
        "select type_timestamp_with_tz, ",
        "cast(type_timestamp_with_tz as varchar) AS ",
        "type_timestamp_with_tz_string ",
        "from (select timestamp '", timestamp_string, " ",
        ifelse(
          type == "Presto",
          timezone,
          tz_to_offset(timezone, dt = as.Date(timestamp_date))
        ),
        "' as type_timestamp_with_tz)"
      )
    ),
    tibble::tibble(
      type_timestamp_with_tz =
        lubridate::with_tz(
          as.POSIXct(timestamp_string, tz = timezone),
          test.output.timezone()
        ),
      type_timestamp_with_tz_string =
        paste0(
          as.character(
            as.POSIXct(timestamp_string, tz = timezone),
            digits = 3
          ), " ",
          ifelse(
            type == "Presto",
            timezone,
            tz_to_offset(timezone, dt = as.Date(timestamp_date))
          )
        )
    )
  )
  expect_s3_class(df.timestamp_with_tz$type_timestamp_with_tz, "POSIXct")
  expect_equal(
    attr(df.timestamp_with_tz$type_timestamp_with_tz[[1]], "tz"),
    test.output.timezone()
  )
}

test_that("output.timezone works", {
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    type = "Presto"
  )
  # session.timezone and output.timezone should be the safe by default
  expect_equal(conn.presto@session.timezone, test.timezone())
  expect_equal(conn.presto@output.timezone, test.timezone())
  dbDisconnect(conn.presto)
  conn.presto <- setup_live_connection(
    session.timezone = test.timezone(),
    output.timezone = test.output.timezone(),
    type = "Presto"
  )
  # session.timezone and output.timezone can be set differently
  expect_equal(conn.presto@session.timezone, test.timezone())
  expect_equal(conn.presto@output.timezone, test.output.timezone())
  .test_time_types(conn.presto, type = "Presto")
  conn.trino <- setup_live_connection(
    session.timezone = test.timezone(),
    output.timezone = test.output.timezone(),
    type = "Trino"
  )
  # session.timezone and output.timezone can be set differently
  expect_equal(conn.trino@session.timezone, test.timezone())
  expect_equal(conn.trino@output.timezone, test.output.timezone())
  .test_time_types(conn.trino, type = "Trino")
})
