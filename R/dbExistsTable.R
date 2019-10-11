# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include dbListTables.R PrestoConnection.R
NULL

#' @rdname PrestoConnection-class
#' @export
setMethod('dbExistsTable',
  c('PrestoConnection', 'character'),
  function(conn, name, ...) {
    return(tolower(name) %in% tolower(dbListTables(conn, pattern=name)))
  }
)
