# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include PrestoConnection.R PrestoCursor.R utility_functions.R
NULL

.request.headers <- function(conn) {
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

.dbSendQuery <- function(conn, statement, ...) {
  url <- paste0(conn@host, ':', conn@port, '/v1/statement')
  status <- 503L
  retries <- 3
  headers <- .request.headers(conn)
  while (status == 503L || (retries > 0 && status >= 400L)) {
    wait()
    post.response <- httr::POST(url, body=enc2utf8(statement), headers)
    status <- as.integer(httr::status_code(post.response))
    if (status >= 400L && status != 503L) {
      retries <- retries - 1
    }
  }
  check.status.code(post.response)
  content <- response.to.content(post.response)
  if (status == 200) {
    if (get.state(content) == 'FAILED') {
      stop.with.error.message(content)
    } else {
      cursor <- PrestoCursor$new(content)
      rv <- new('PrestoResult',
        post.response=post.response,
        statement=statement,
        session.timezone=conn@session.timezone,
        cursor=cursor,
        session=conn@session
      )
    }
  } else {
    stop('Unknown error, status code:', status, ', response: ', content)
  }
  return(rv)
}

#' @rdname PrestoConnection-class
#' @export
setMethod('dbSendQuery', c('PrestoConnection', 'character'), .dbSendQuery)
