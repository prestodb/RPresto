# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

.verified.timezone <- function(conn, session.timezone) {
  system.timezone <- Sys.timezone()

  connection.timezone <- dbGetQuery(
    conn,
    'SELECT CURRENT_TIMEZONE() AS connection_timezone'
  )[['connection_timezone']]

  if (is.null(session.timezone)) {
    if (is.na(system.timezone)) {
      warning('No timezone specified for the session, defaulting to ',
        connection.timezone, '. ',
        'Special care is needed when handling timestamps without ',
        'explicit time zones'
      )
    } else {
      # Presto should error out if this condition is not true
      stopifnot(connection.timezone == system.timezone)
      message('Using inferred time zone ', connection.timezone,
        ' for the session')
    }
  } else {
    # Presto should error out if this condition is not true
    stopifnot(session.timezone == connection.timezone)
  }
  return(connection.timezone)
}
