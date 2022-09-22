# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

library(DBI)

iris.sql <- function() {
  .row.to.select <- function(...) {
    items <- list(...)
    types <- lapply(items, data.class)
    presto.types <- ifelse(unlist(types) == "numeric", "DOUBLE", "VARCHAR")
    values <- unlist(lapply(items, as.character))

    columns <- paste("CAST('", values, "' AS ", presto.types, ') AS "', names(items), '"', sep = "", collapse = ", ")
    return(paste("SELECT", columns))
  }
  rows <- do.call("mapply", c(FUN = .row.to.select, as.list(iris)))

  sql <- paste("(", paste(rows, collapse = " UNION ALL "), ") iris")

  return(sql)
}

expect_equal_data_frame <- function(r, e, ...) {
  expect_equal(r, e, ...)
  data.classes <- unlist(lapply(e, data.class))
  for (column in which(data.classes %in% "POSIXct")) {
    # all.equal.POSIXt does not check for timezone equality, even when
    # check.attributes=TRUE
    expect_equal(
      attr(r[[column]], "tzone"),
      attr(e[[column]], "tzone")
    )
  }
}

test.timezone <- function() {
  return("Asia/Kathmandu")
}

test.output.timezone <- function() {
  return("America/New_York")
}

test.locale <- function() {
  if (.Platform[["OS.type"]] == "windows") {
    return("Turkish_Turkey.1254")
  }
  return("tr_TR.iso8859-9")
}

with_locale <- function(locale, f) {
  wrapped <- function(desc, ...) {
    old.locale <- Sys.getlocale("LC_CTYPE")
    tryCatch(
      {
        Sys.setlocale("LC_CTYPE", locale)
      },
      warning = function(w) {
        warning.message <- conditionMessage(w)
        if (!grepl(
          "OS reports request to set locale to .* cannot be honored",
          warning.message
        )) {
          warning(w)
        }
      }
    )

    on.exit(Sys.setlocale("LC_CTYPE", old.locale), add = TRUE)
    new.locale <- Sys.getlocale("LC_CTYPE")
    if (new.locale != locale) {
      return(test_that(desc = desc, {
        skip(paste0(
          "Cannot set locale to ", locale,
          "it is set at: ", new.locale
        ))
      }))
    }
    return(f(desc = desc, ...))
  }
  return(wrapped)
}

# Note that you need to wrap your test_that call with with_locale if you
# use the data returned here for comparison
data.frame.with.all.classes <- function(row.indices) {
  old.locale <- Sys.getlocale("LC_CTYPE")
  Sys.setlocale("LC_CTYPE", test.locale())
  on.exit(Sys.setlocale("LC_CTYPE", old.locale), add = TRUE)

  e <- tibble::tibble(
    logical = c(TRUE, FALSE),
    integer = c(1L, 2L),
    numeric = c(0.0, 1.0),
    character1 = c("0", "1.414"),
    character2 = c("", "z"),
    raw = list(raw(0), raw(0)),
    Date = as.Date(c("2015-03-01", "2015-03-02")),
    POSIXct_no_time_zone = as.POSIXct(
      c("2015-03-01 12:00:00", "2015-03-02 12:00:00.321"),
      tz = test.timezone()
    ),
    POSIXct_with_time_zone = as.POSIXct(
      c("2015-03-01 12:00:00", "2015-03-02 12:00:00.321"),
      tz = "Europe/Paris"
    ),
    # The first element is 'ıİÖğ' in iso8859-9 encoding,
    # and the second 'Face with tears of joy' in UTF-8
    character3 = c("\xFD\xDD\xD6\xF0", "\u1F602"),
    list_unnamed = list(list(1, 2), list()),
    list_named = list(list(a = 1, b = 2), list())
  )
  if (!missing(row.indices)) {
    rv <- e[row.indices, , drop = FALSE]
    return(rv)
  }
  return(e)
}

data.to.list <- function(data) {
  # Change POSIXct representation, otherwise microseconds
  # are chopped off in toJSON
  old.digits.secs <- options("digits.secs" = 3)
  on.exit(options(old.digits.secs), add = TRUE)

  presto.types <- lapply(data, function(l) {
    if (is.list(l)) {
      rs.class <- data.class(l[[1]])
      if (rs.class == "raw") {
        rv <- "varbinary"
      } else if (rs.class == "list") {
        rv <- vector(mode = "character")
        if (is.null(names(l[[1]]))) {
          rv[1] <- "array"
          if (length(l[[1]]) > 0) {
            rv[2] <- dbDataType(RPresto::Presto(), l[[1]][[1]])
          } else {
            rv[2] <- "double"
          }
        } else {
          rv[1] <- "map"
          if (length(l[[1]]) > 0) {
            rv[2] <- dbDataType(RPresto::Presto(), names(l[[1]])[1])
            rv[3] <- dbDataType(RPresto::Presto(), l[[1]][[1]])
          } else {
            rv[2] <- "varchar"
            rv[3] <- "double"
          }
        }
      } else {
        stop("Unsupported mock data type: ", rs.class)
      }
    } else {
      rv <- dbDataType(RPresto::Presto(), l)
    }
    return(rv)
  })

  column.data <- list()
  for (i in seq_along(presto.types)) {
    presto.type <- stringi::stri_trans_tolower(
      presto.types[[i]],
      "en_US.UTF-8"
    )
    column.data[[i]] <- list(
      name = jsonlite::unbox(colnames(data)[i]),
      type = jsonlite::unbox(presto.type[1]),
      typeSignature = list(
        rawType = jsonlite::unbox(presto.type[1]),
        typeArguments = if (length(presto.type) == 1) {
          list()
        } else {
          if (presto.type[1] == "array") {
            list(
              list(rawType = jsonlite::unbox(presto.type[2]))
            )
          } else {
            list(
              list(rawType = jsonlite::unbox(presto.type[2])),
              list(rawType = jsonlite::unbox(presto.type[3]))
            )
          }
        },
        literalArguments = list()
      )
    )
    if (
      length(presto.type) == 1 &&
        presto.type == "timestamp with time zone" &&
        NROW(data)
    ) {
      # toJSON ignores the timezone attribute
      data[[i]] <- paste(data[[i]], attr(data[[i]], "tzone"))
    } else if (presto.type[1] %in% c("array", "map")) {
      # Lists need to be unboxed
      data[[i]] <- lapply( # Apply to each row
        data[[i]],
        function(l) lapply(l, jsonlite::unbox) # Apply to each item
      )
    }
  }
  return(list(column.data = column.data, data = data))
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

read_credentials <- function() {
  if (!file.exists("credentials.dcf")) {
    skip(paste0(
      "credential file missing, please create a file ",
      system.file("tests", "testthat", "credentials.dcf", package = "RPresto"),
      ' with fields "host", "port", "source", "catalog", "schema" to do live testing'
    ))
  }
  dcf <- read.dcf("credentials.dcf")
  credentials <- list(
    host = as.vector(dcf[1, "host"]),
    port = as.integer(as.vector(dcf[1, "port"])),
    catalog = as.vector(dcf[1, "catalog"]),
    schema = as.vector(dcf[1, "schema"]),
    iris_table_name = as.vector(dcf[1, "iris_table_name"]),
    source = as.vector(dcf[1, "source"])
  )
  return(credentials)
}

setup_live_connection <- function(session.timezone = "",
                                  parameters = list(),
                                  extra.credentials = "",
                                  bigint = c("integer", "integer64", "numeric", "character"),
                                  ...,
                                  type = "Presto") {
  skip_on_cran()
  if (type == "Presto") {
    credentials <- read_credentials()
    conn <- dbConnect(RPresto::Presto(),
      schema = credentials$schema,
      catalog = credentials$catalog,
      host = credentials$host,
      port = credentials$port,
      source = credentials$source,
      session.timezone = session.timezone,
      parameters = parameters,
      extra.credentials = extra.credentials,
      user = Sys.getenv("USER"),
      bigint = bigint,
      ...
    )
  } else if (type == "Trino") {
    conn <- dbConnect(RPresto::Presto(),
      use.trino.headers = TRUE,
      schema = "default",
      catalog = "memory",
      host = "http://localhost",
      port = 8090,
      source = "RPresto_test",
      session.timezone = session.timezone,
      parameters = parameters,
      extra.credentials = extra.credentials,
      user = Sys.getenv("USER"),
      bigint = bigint,
      ...
    )
  } else {
    stop("Connection type is not Presto or Trino.", call. = FALSE)
  }
  return(conn)
}

setup_live_dplyr_connection <- function(session.timezone = "",
                                        parameters = list(),
                                        extra.credentials = "",
                                        bigint = c("integer", "integer64", "numeric", "character"),
                                        ...,
                                        type = "Presto") {
  skip_on_cran()

  credentials <- read_credentials()
  db <- src_presto(
    con = setup_live_connection(
      session.timezone,
      parameters,
      extra.credentials,
      bigint,
      ...,
      type = type
    )
  )
  return(list(db = db, iris_table_name = credentials[["iris_table_name"]]))
}

setup_mock_connection <- function() {
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SELECT current_timezone() AS tz",
        data = data.frame(tz = Sys.timezone(), stringsAsFactors = FALSE)
      )
    ),
    {
      mock.conn <- dbConnect(
        RPresto::Presto(),
        schema = "test",
        catalog = "catalog",
        host = "http://localhost",
        port = 8000,
        source = "RPresto Test",
        session.timezone = test.timezone(),
        user = Sys.getenv("USER")
      )
      return(mock.conn)
    }
  )
}

setup_mock_dplyr_connection <- function() {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  with_mock(
    `httr::POST` = mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SELECT current_timezone() AS tz",
        data = data.frame(tz = Sys.timezone(), stringsAsFactors = FALSE)
      )
    ),
    {
      db <- src_presto(
        schema = "test",
        catalog = "catalog",
        host = "http://localhost",
        port = 8000,
        source = "RPresto Test",
        user = Sys.getenv("USER"),
        session.timezone = test.timezone(),
        parameters = list()
      )

      return(list(db = db, iris_table_name = "iris_table"))
    }
  )
}

# helper functions
data_frame <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}

get_nrow <- function(con, tbl) {
  df <- dbGetQuery(con, paste("SELECT COUNT(*) AS n FROM", tbl))
  return(df$n)
}

tz_to_offset_sec <- function(timezone, dt = Sys.Date()) {
  offset <- as.integer(lubridate::force_tz(as.POSIXlt(dt), tz = timezone)$gmtoff)
  if (is.null(offset)) offset <- 0L
  return(offset)
}

tz_to_offset_hr <- function(timezone, dt = Sys.Date()) {
  offset_sec <- tz_to_offset_sec(timezone, dt)
  as.integer(floor(abs(offset_sec) / 3600))
}

tz_to_offset_min <- function(timezone, dt = Sys.Date()) {
  offset_sec <- tz_to_offset_sec(timezone, dt)
  as.integer((abs(offset_sec) %% 3600) / 60)
}

tz_to_offset <- function(timezone, dt = Sys.Date()) {
  offset_sec <- tz_to_offset_sec(timezone, dt)
  offset_sign <- ifelse(offset_sec >= 0, "+", "-")
  offset_hour <- tz_to_offset_hr(timezone, dt)
  offset_hour_string <- stringi::stri_pad_left(offset_hour, 2, "0")
  offset_min <- tz_to_offset_min(timezone, dt)
  offset_min_string <- stringi::stri_pad_left(offset_min, 2, "0")
  paste0(offset_sign, offset_hour_string, ":", offset_min_string)
}
