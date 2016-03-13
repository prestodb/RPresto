# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('session.timezone')

source('utilities.R')

test_that('session.timezone works', {
  current.timezone <- Sys.timezone()
  if (!is.na(current.timezone)) {
    on.exit(Sys.setenv(TZ=current.timezone))
  } else {
    on.exit(Sys.unsetenv('TZ'))
  }

  Sys.setenv(TZ='Asia/Tokyo')

  conn <- setup_live_connection(session.timezone='Europe/Istanbul')
  expect_equal(conn@session.timezone, 'Europe/Istanbul')

  # At 2014-10-26 04:00:00 Istanbul switched back to Standard time from
  # daylight saving. Tokyo did not observe daylight saving in 2014.
  timestamp_6am <- '2014-10-26 06:00:00'
  timestamp_1pm <- '2014-10-26 13:00:00'
  timestamp_1am <- '2014-10-26 01:00:00'
  timestamp_8pm <- '2014-10-26 20:00:00'
  tokyo_1pm <- as.POSIXct(timestamp_1pm)
  tokyo_1pm_timestamp <- as.character(tokyo_1pm) # 2014-10-26 13:00:00
  tokyo_1pm_unixtime <- as.numeric(tokyo_1pm)

  tokyo_6am <- as.POSIXct(timestamp_6am)
  tokyo_6am_timestamp <- as.character(tokyo_6am)

  tokyo_8pm <- as.POSIXct(timestamp_8pm)
  tokyo_8pm_timestamp <- as.character(tokyo_8pm)

  istanbul_1pm <- as.POSIXct(timestamp_1pm, tz='Europe/Istanbul')
  istanbul_1pm_timestamp <- as.character(istanbul_1pm)
  istanbul_1pm_unixtime <- as.numeric(istanbul_1pm)

  istanbul_6am <- as.POSIXct(timestamp_6am, tz='Europe/Istanbul')
  istanbul_6am_timestamp <- as.character(istanbul_6am)
  istanbul_6am_unixtime <- as.numeric(istanbul_6am)

  istanbul_1am <- as.POSIXct(timestamp_1am, tz='Europe/Istanbul')

  expect_equal(tokyo_1pm_timestamp, istanbul_1pm_timestamp)
  expect_equal(istanbul_1pm_unixtime - tokyo_1pm_unixtime, 7 * 60 * 60)

  expect_equal(tokyo_1pm_unixtime, istanbul_6am_unixtime)

  rv <- dbGetQuery(conn,
    sprintf("SELECT
      -- Should have Istanbul timezone
      FROM_UNIXTIME(%f) AS \"from_unixtime\",
      TO_UNIXTIME(TIMESTAMP '%s') AS \"to_unixtime\",
      ABS(%f - TO_UNIXTIME(TIMESTAMP '%s')) < 1 AS \"unixtime_same\",
      TIMESTAMP '%s' AS \"timestamp\",
      TIMESTAMP '%s' - INTERVAL '13' HOUR AS \"dst_timestamp\",
      -- Without proper time zone parsing in R, this will return true
      -- since the string returned by presto will be parsed in Tokyo timezone
      TIMESTAMP '%s' < FROM_UNIXTIME(%f) AS \"tokyo_vs_istanbul\",
      DATE_FORMAT(
        -- Note that timestamp is first parsed in session timezone, then
        -- changed to the given time zone
        TIMESTAMP '%s' AT TIME ZONE 'Asia/Tokyo',
        '%%Y-%%m-%%d %%H:%%i:%%s'
      ) AS \"date_format\",
      DATE_FORMAT(
        TIMESTAMP '2014-10-26 00:00:00' AT TIME ZONE 'Asia/Tokyo',
        '%%Y-%%m-%%d %%H:%%i:%%s'
      ) AS \"date_format_before_standard_time\"",
      tokyo_1pm_unixtime,
      timestamp_1pm,
      tokyo_1pm_unixtime, timestamp_1pm,
      timestamp_1pm,
      timestamp_1pm,
      timestamp_1pm, tokyo_1pm_unixtime + 1,
      timestamp_1pm
    )
  )
  ev <- data.frame(
    from_unixtime=istanbul_6am,
    to_unixtime=istanbul_1pm_unixtime,
    unixtime_same=FALSE, # tokyo_1pm_unixtime vs istanbul_1pm_unixtime
    timestamp=istanbul_1pm,
    dst_timestamp=istanbul_1am, # NOT midnight, due to DST
    tokyo_vs_istanbul=FALSE,
    date_format=tokyo_8pm_timestamp,
    date_format_before_standard_time=tokyo_6am_timestamp,
    stringsAsFactors=FALSE
  )
  expect_equal_data_frame(rv, ev)
})
