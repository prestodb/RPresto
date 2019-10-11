# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

.request_headers <- function(conn) {
  return(httr::add_headers(
    "X-Presto-User"= conn@user,
    "X-Presto-Catalog"= conn@catalog,
    "X-Presto-Schema"= conn@schema,
    "X-Presto-Source"= conn@source,
    "X-Presto-Time-Zone" = conn@session.timezone,
    "User-Agent"= getPackageName(),
    "X-Presto-Session"=conn@session$parameterString()
  ))
}
