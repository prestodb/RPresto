# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

.description_from_info <- function(info) {
  return(paste0(
    'presto ',
    ' [',
    info[['schema']],
    ':',
    info[['catalog']],
    ' | ',
    info[['user']],
    '@',
    info[['host']],
    ':',
    info[['port']],
    ']'
  ))
}
