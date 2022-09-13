# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

.request_headers <- function(conn) {
  if (isTRUE(conn@use.trino.headers)) {
    headers <- httr::add_headers(
      "X-Trino-User" = conn@user,
      "X-Trino-Catalog" = conn@catalog,
      "X-Trino-Schema" = conn@schema,
      "X-Trino-Source" = conn@source,
      "X-Trino-Time-Zone" = conn@session.timezone,
      "User-Agent" = methods::getPackageName(),
      "X-Trino-Session" = conn@session$parameterString(),
      "X-Trino-Extra-Credential" = conn@extra.credentials
    )
  } else {
    headers <- httr::add_headers(
      "X-Presto-User" = conn@user,
      "X-Presto-Catalog" = conn@catalog,
      "X-Presto-Schema" = conn@schema,
      "X-Presto-Source" = conn@source,
      "X-Presto-Time-Zone" = conn@session.timezone,
      "User-Agent" = methods::getPackageName(),
      "X-Presto-Session" = conn@session$parameterString(),
      "X-Presto-Extra-Credential" = conn@extra.credentials
    )
  }
  request_combine <- utils::getFromNamespace("request_combine", "httr")
  return(request_combine(headers, conn@request.config))
}
