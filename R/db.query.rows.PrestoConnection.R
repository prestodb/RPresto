# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @export
db_query_rows.PrestoConnection <- function(con, sql) {
  # We shouldn't be doing a COUNT(*) over arbitrary tables because Hive tables
  # can be prohibitively long. There may be something smarter we can do for
  # smaller tables though.
  return(NA)
}

