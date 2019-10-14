# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

check.status.code <- function(response) {
  status <- httr::status_code(response)
  if (status >= 400 && status < 500) {
    text.content <- httr::content(response, as = "text", encoding='UTF-8')
    if (is.null(text.content) || !nzchar(text.content)) {
      httr::stop_for_status(status)
    }
    stop('Received error response (HTTP ', status, '): ', text.content)
  }
}

response.to.content <- function (response)  {
  text.content <- httr::content(response, as = "text", encoding='UTF-8')
  return(jsonlite::fromJSON(text.content, simplifyVector = FALSE))
}

wait <- function () {
  # sleep 50 - 100 ms
  Sys.sleep(stats::runif(n = 1, min = 50, max = 100) / 1000)
}

get.state <- function (content) {
  if (is.null(content[['stats']])
      || is.null(content[['stats']][['state']])
  ) {
    stop('No state information in content')
  }
  return(content[['stats']][['state']])
}

stop.with.error.message <- function (content) {
  query.id <- content[['id']]
  message <- content[['error']][['message']]
  stop("Query ", query.id, " failed: ", message)
}
