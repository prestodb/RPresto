# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

"%||%" <- function(x, y) if (is.null(x)) {
  return(y)
} else {
  return(x)
}

wait <- function() {
  # sleep 50 - 100 ms
  Sys.sleep(stats::runif(n = 1, min = 50, max = 100) / 1000)
}

.stop.with.error.message <- function(content) {
  query.id <- content$id
  message <- content$error$message
  stop("Query ", query.id, " failed: ", message)
}

.get.content.state <- function(content) {
  if (is.null(content$stats) ||
    is.null(content$stats$state)
  ) {
    stop("No state information in content")
  }
  return(content$stats$state)
}

.check.response.status <- function(response) {
  status <- httr::status_code(response)
  if (status != 200) {
    text.content <- httr::content(response, as = "text", encoding = "UTF-8")
    if (is.null(text.content) || !nzchar(text.content)) {
      httr::stop_for_status(status)
    }
    stop("Received error response (HTTP ", status, "): ", text.content)
  }
}

# Similar to the PrestoRequest.process method in the Presto Python client
.response.to.content <- function(response) {
  # response can be application/octet-stream MIME type with no auto parser
  text.content <- httr::content(response, as = "text", encoding = "UTF-8")
  content <- jsonlite::fromJSON(text.content, simplifyVector = FALSE)
  return(content)
}

#' Class to encapsulate a Presto query
#'
#' This reference class (so that the object can be passed by reference and
#' modified) encapsulates the lifecycle of a Presto query from its inception
#' (by providing a PrestoConnection and a query statement) to the steps it takes
#' to execute (i.e. an initial POST request and subsequent GET requests).
#'
#' This is similar to the PrestoQuery class defined in the Presto Python client
#' @slot .conn A PrestoConnection object
#' @slot .statement The query statement
#' @slot .id The query ID returned after the first POST request
#' @slot .timestamp The timestamp of the query execution
#' @slot .bigint How BIGINT fields should be converted to an R class
#' @slot .state The query state. This changes every time the query advances to
#'       the next stage
#' @slot .next.uri The URI that specifies the next endpoint to send the GET
#'       request
#' @slot .info.uri The information URI
#' @slot .stats Query stats. This changes every time the query advances to the
#'       next stage
#' @slot .response HTTP request response. This changes when the query advances
#' @slot .content Parsed content from the HTTP request response
#' @slot .fetched.row.count How many rows of data have been fetched to R
#' @slot .post.data.fetched A boolean flag indicating if data returned from the
#'       POST request has been fetched
#' @slot .quiet If a progress bar should be shown for long queries (which run
#'       for more than 2 seconds. Default to `NA` which turns on the
#'       progress bar for interactive queries.
#' @keywords internal
#' @importFrom progress progress_bar
PrestoQuery <- setRefClass("PrestoQuery",
  fields = c(
    # immutable once the query is created
    ".conn",
    ".statement",
    ".id",
    ".timestamp",
    ".bigint",
    # mutable depending on the stage of the query
    ".state",
    ".next.uri",
    ".info.uri",
    ".stats",
    ".response",
    ".content",
    # mutable when the result is fetched
    ".fetched.row.count",
    ".post.data.fetched",
    ".progress",
    ".quiet"
  ),
  methods = list(
    initialize = function(
      conn, statement, ..., quiet = getOption("rpresto.quiet")
    ) {
      dots <- list(...)
      if ("bigint" %in% names(dots)) {
        bigint <- dots$bigint
        stopifnot(bigint %in% c("integer", "integer64", "numeric", "character"))
      } else {
        bigint <- NULL
      }
      if (is.na(quiet)) {
        quiet <- !interactive()
      }
      initFields(
        .conn = conn,
        .statement = statement,
        .state = "",
        .fetched.row.count = 0L,
        .post.data.fetched = NA,
        .bigint = bigint,
        .quiet = quiet
      )
    },
    # Getter functions
    id = function() {
      return(.id)
    },
    infoUri = function() {
      return(.info.uri)
    },
    nextUri = function() {
      return(.next.uri)
    },
    content = function() {
      return(.content)
    },
    # Setter functions
    response = function(value) {
      if (!missing(value)) {
        .response <<- value
      }
      invisible(.response)
    },
    state = function(new.state) {
      if (!missing(new.state)) {
        .state <<- new.state %||% ""
      }
      invisible(.state)
    },
    stats = function(new.stats) {
      if (!missing(new.stats)) {
        .stats <<- new.stats
      }
      invisible(.stats)
    },
    postDataFetched = function(value) {
      if (!missing(value)) {
        .post.data.fetched <<- value
      }
      invisible(.post.data.fetched)
    },
    fetchedRowCount = function(value) {
      if (!missing(value)) {
        .fetched.row.count <<- .fetched.row.count + as.integer(value)
      }
      invisible(.fetched.row.count)
    },
    # More complex functions
    getContentState = function() {
      return(.get.content.state(.content))
    },
    stopWithErrorMessage = function() {
      .stop.with.error.message(.content)
    },
    checkResponseStatus = function() {
      .check.response.status(.response)
    },
    checkContentState = function() {
      if (getContentState() == "FAILED") {
        state("FAILED")
        stats(.content$stats)
        stopWithErrorMessage()
      }
    },
    responseToContent = function() {
      checkResponseStatus()
      .content <<- .response.to.content(.response)
      checkContentState()
    },
    computeProgress = function() {
      total.splits <- .stats$totalSplits
      if ((.stats$scheduled %||% FALSE) && total.splits > 0) {
        return(.stats$completedSplits / total.splits)
      } else {
        return(0)
      }
    },
    # Similar to the PrestoRequest.post method in the Presto Python client
    # Make a POST request to Presto and store the response
    post = function() {
      url <- paste0(.conn@host, ":", .conn@port, "/v1/statement")
      status <- 503L
      retries <- 3
      headers <- .request_headers(.conn)
      while (status == 503L || (retries > 0 && status >= 400L)) {
        wait()
        post.response <- httr::POST(
          url,
          body = enc2utf8(.statement),
          config = headers
        )
        status <- as.integer(httr::status_code(post.response))
        if (status >= 400L && status != 503L) {
          retries <- retries - 1
        }
      }
      response(post.response)
    },
    # Make a GET request to Presto using the next URI and store the response
    get = function(num.retry = 3) {
      headers <- .request_headers(.conn)
      get.response <- tryCatch(
        {
          response <- httr::GET(.next.uri, config = headers)
          if (httr::status_code(response) >= 400L) {
            # stop_for_status also fails for 300 <= status < 400
            # so we need the if condition
            httr::stop_for_status(response)
          }
          response
        },
        error = function(e) {
          if (num.retry == 0) {
            stop(
              "There was a problem with the request ",
              "and we have exhausted our retry limit for uri: ", .next.uri
            )
          }
          message(
            'GET call failed with error: "', conditionMessage(e),
            '", retrying [', 4 - num.retry, "/3]\n"
          )
          wait()
          httr::handle_reset(.next.uri)
          return(get(num.retry - 1))
        }
      )
      response(get.response)
    },
    # Execute the query and update information
    execute = function() {
      result <- methods::new(
        "PrestoResult",
        statement = .statement,
        connection = .conn,
        query = .self,
        bigint = .bigint %||% .conn@bigint
      )
      post()
      .timestamp <<-
        lubridate::with_tz(.response$date, tz = .conn@session.timezone)
      responseToContent()
      .id <<- .content$id
      updateQuery()
      if (.quiet) {
        .progress <<- list(
          tick = function(...) {},
          update = function(...) {},
          terminate = function(...) {},
          finished = TRUE
        )
      } else {
        .progress <<- progress::progress_bar$new(
          format = "  query :state [:bar] :percent in :elapsed",
          clear = FALSE,
          total = 100,
          width = 80,
          # Suppress progress bar for very short queries
          show_after = 2
        )
        # Immediately tick() the progress bar for it to show up
        .progress$tick(0, tokens = list(state = .state))
      }
      df <- extractData()
      if ((NROW(df) | NCOL(df)) != 0) {
        # Store the extracted POST data in the result object
        # This should call the setter method from PrestoResult once it gains one
        result@post.data <- df
        if (NROW(df) != 0) {
          # dbFetch() should check this flag to return the post data if it
          # hasn't been fetched
          .post.data.fetched <<- FALSE
        }
      }
      return(result)
    },
    # A cleaner design would be to pass the response
    # directly as an argument, but that would mean parsing
    # the content multiple times.
    updateQuery = function() {
      .next.uri <<- .content$nextUri %||% ""
      .info.uri <<- .content$infoUri %||% ""
      state(getContentState())
      stats(.content$stats)
      updateSession()
    },
    # Call get() to fetch the next batch of query result and extract the data
    fetch = function() {
      df <- tibble::tibble()
      if (!hasCompleted()) {
        tryCatch(
          get(),
          error = function(e) {
            state("FAILED")
            .progress$update(1, tokens = list(state = .state))
            .progress$terminate()
            stop(simpleError(
              paste0(
                "Cannot fetch ", .next.uri, ", ",
                "error: ", conditionMessage(e)
              )
            ))
          }
        )
        responseToContent()
        updateQuery()
        if (!.progress$finished) {
          .progress$update(
            ratio = computeProgress(),
            tokens = list(state = .state)
          )
        }
        df <- extractData()
      }
      return(df)
    },
    updateSession = function() {
      if (!is.null(.content$updateType)) {
        switch(.content$updateType,
          "SET SESSION" = {
            properties <- httr::headers(.response)[["x-presto-set-session"]]
            if (!is.null(properties)) {
              for (pair in strsplit(properties, ",", fixed = TRUE)) {
                pair <- unlist(strsplit(pair, "=", fixed = TRUE))
                .conn@session$setParameter(pair[1], pair[2])
              }
            }
          },
          "RESET SESSION" = {
            properties <- httr::headers(.response)[["x-presto-clear-session"]]
            if (!is.null(properties)) {
              for (key in strsplit(properties, ",", fixed = TRUE)) {
                .conn@session$unsetParameter(key)
              }
            }
          }
        )
      }
    },
    extractData = function() {
      df <- extract.data(
        .content,
        session.timezone = .conn@session.timezone,
        output.timezone = .conn@output.timezone,
        timestamp = .timestamp,
        bigint = .bigint %||% .conn@bigint
      )
      return(df)
    },
    hasCompleted = function() {
      # .post.data.fetched is FALSE when POST returns some data but the data
      # hasn't been fetched using dbFetch() yet. Usually in this situation
      # there is still some next URI to GET, so it's usually redundant to check
      # .post.data.fetched (i.e. POST but not fetched).
      return(.next.uri == "" & !isFALSE(.post.data.fetched))
    }
  )
)
