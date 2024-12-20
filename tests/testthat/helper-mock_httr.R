# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

check_header <- function(header, value, ...) {
  arguments <- list(...)
  if (!"config" %in% names(arguments)) {
    stop("config argument for request not set")
  }
  config <- arguments[["config"]]
  if (inherits(config, "request") &&
      !is.null(config[["headers"]]) &&
      !is.null(config[["headers"]][[header]]) &&
      config[["headers"]][[header]] == value) {
    # OK
  } else {
    stop("Expected value ", value, " for header ", header, " not set")
  }
}

mock_httr_response <- function(url,
                               status_code,
                               state,
                               request_body,
                               data,
                               extra_content,
                               next_uri,
                               info_uri,
                               query_id) {
  if (!missing(extra_content)) {
    content <- extra_content
  } else {
    content <- list()
  }
  if (!missing(state)) {
    content[["stats"]] <- list(state = jsonlite::unbox(state))
  }
  if (missing(query_id)) {
    content[["id"]] <- jsonlite::unbox(gsub("[:/]", "_", url))
  } else {
    content[["id"]] <- jsonlite::unbox(query_id)
  }

  if (!missing(next_uri)) {
    content[["nextUri"]] <- jsonlite::unbox(next_uri)
  }

  if (!missing(info_uri)) {
    content[["infoUri"]] <- jsonlite::unbox(info_uri)
  }

  if (!missing(data)) {
    content.info <- data.to.list(data)
    content[["columns"]] <- content.info[["column.data"]]
    content[["data"]] <- content.info[["data"]]
  }

  # Change POSIXct representation, otherwise microseconds
  # are chopped off in toJSON
  old.digits.secs <- options("digits.secs" = 3)
  on.exit(options(old.digits.secs), add = TRUE)

  rv <- list(
    url = url,
    response = structure(
      list(
        url = url,
        status_code = status_code,
        headers = list(
          "content-type" = "application/json"
        ),
        content = charToRaw(jsonlite::toJSON(content, dataframe = "values"))
      ),
      class = "response"
    )
  )
  if (!missing(request_body)) {
    rv[["request_body"]] <- request_body
  }
  return(rv)
}

mock_httr_replies <- function(...) {
  response.list <- list(...)
  names(response.list) <- unlist(lapply(response.list, function(l) l[["url"]]))
  f <- function(url, body, ...) {
    check_header("X-Presto-User", Sys.getenv("USER"), ...)
    check_header("User-Agent", "RPresto", ...)
    # Iterate over all specified response mocks and see if both url and body
    # match
    for (i in seq_along(response.list)) {
      item <- response.list[[i]]

      url.matches <- item[["url"]] == url
      if (missing(body)) {
        body.matches <- is.null(item[["request_body"]])
      } else {
        body.matches <- (
          !is.null(item[["request_body"]]) &&
            grepl(item[["request_body"]], body)
        )
      }

      if (url.matches && body.matches) {
        return(item[["response"]])
      } else if (
        url == "http://localhost:8000/v1/statement" &&
        body == "SELECT current_timezone() AS tz"
      ) {
        item <- mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "SELECT current_timezone() AS tz",
          data = data.frame(tz = Sys.timezone(), stringsAsFactors = FALSE),
        )
        return(item[["response"]])
      }
    }

    stop(paste0(
      "No mocks for url: ", url,
      if (!missing(body)) {
        paste0(", request_body: ", body)
      }
    ))
  }
  return(f)
}
