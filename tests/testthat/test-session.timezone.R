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

  Sys.unsetenv('TZ')
  expect_warning(
    setup_live_connection(session.timezone=NULL),
    'No timezone specified for the session, defaulting to'
  )
  expect_error(
    setup_live_connection(session.timezone="invalid_timezone"),
    'Time zone invalid_timezone is not supported'
  )

  Sys.setenv(TZ='Asia/Tokyo')
  expect_message(
    setup_live_connection(),
    'Using inferred time zone Asia/Tokyo for the session'
  )

  conn <- setup_live_connection(session.timezone='Europe/Istanbul')
  expect_equal(conn@session.timezone, 'Europe/Istanbul')

  # At 2014-10-26 04:00:00 Istanbul switched back to Standard time from
  # daylight saving. Tokyo did not observe daylight saving in 2014.
  time <- as.POSIXct('2014-10-26 13:00:00')
  timestamp <- as.character(time)
  unixtime <- as.numeric(time)

  rv <- dbGetQuery(conn,
    sprintf("SELECT
      FROM_UNIXTIME(%f) AS \"from_unixtime\",
      TO_UNIXTIME(TIMESTAMP '%s') AS \"to_unixtime\",
      ABS(%f - TO_UNIXTIME(TIMESTAMP '%s')) < 1 AS \"unixtime_same\",
      TIMESTAMP '%s' AS \"timestamp\",
      TIMESTAMP '%s' - INTERVAL '13' HOUR AS \"dst_timestamp\",
      -- Without proper time zone handling, this will return true
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
      unixtime,
      timestamp,
      unixtime, timestamp,
      timestamp,
      timestamp,
      timestamp, unixtime + 1,
      timestamp
    )
  )
  ev <- data.frame(
    from_unixtime=structure(
      unixtime,
      class=c('POSIXct', 'POSIXt'),
      tzone='Europe/Istanbul'
    ),
    to_unixtime=as.numeric(as.POSIXct(timestamp, tz='Europe/Istanbul')),
    unixtime_same=FALSE,
    timestamp=as.POSIXct(timestamp, tz='Europe/Istanbul'),
    dst_timestamp=as.POSIXct('2014-10-26 01:00:00', tz='Europe/Istanbul'),
    tokyo_vs_istanbul=FALSE,
    date_format='2014-10-26 20:00:00',
    date_format_before_standard_time='2014-10-26 06:00:00',
    stringsAsFactors=FALSE
  )
  expect_equal_data_frame(rv, ev)
})
