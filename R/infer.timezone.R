# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

.infer.timezone <- function(session.timezone) {
    if (!is.null(session.timezone)) {
      return(session.timezone)
    }

    system.timezone <- Sys.timezone()
    if (!is.na(system.timezone)) {
      return(system.timezone)
    }

    return('')
}
