# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

library(DBI)

iris.sql <- function() {
  .row.to.select <- function(...) {
    items <- list(...)
    types <- lapply(items, data.class)
    presto.types <- ifelse(unlist(types) == 'numeric', 'DOUBLE', 'VARCHAR')
    values <- unlist(lapply(items, as.character))

    columns <- paste("CAST('", values, "' AS ", presto.types, ') AS "', names(items), '"', sep='', collapse=', ')
    return(paste('SELECT', columns))
  }
  rows <- do.call('mapply', c(FUN=.row.to.select, as.list(iris)))

  sql <- paste('(', paste(rows, collapse=' UNION ALL '), ') iris')

  return(sql)
}

expect_equal_data_frame <- function(r, e, ...) {
  expect_equal(r, e, ...)
  data.classes <- unlist(lapply(e, data.class))
  for (column in which(data.classes %in% 'POSIXct')) {
    # all.equal.POSIXt does not check for timezone equality, even when
    # check.attributes=TRUE
    expect_equal(
      attr(r[[column]], 'tzone'),
      attr(e[[column]], 'tzone')
    )
  }
}

test.timezone <- function() { return('Asia/Kathmandu') }

data.frame.with.all.classes <- function(row.indices) {
  old.locale <- Sys.getlocale('LC_CTYPE')
  Sys.setlocale('LC_CTYPE', test.locale())
  on.exit(Sys.setlocale('LC_CTYPE', old.locale), add=TRUE)

  e <- data.frame(
    c(TRUE, FALSE),
    c(1L, 2L),
    c(0.0, 1.0),
    c('', 'z'),
    rep(NA, 2),
    as.Date(c('2015-03-01', '2015-03-02')),
    as.POSIXct(
      c('2015-03-01 12:00:00', '2015-03-02 12:00:00.321'),
      tz=test.timezone()
    ),
    as.POSIXct(
      c('2015-03-01 12:00:00', '2015-03-02 12:00:00.321'),
      tz='UTC'
    ),
    # The first element is 'ıİÖğ' in iso8859-9 encoding,
    # and the second 'Face with tears of joy' in UTF-8
    c('\xFD\xDD\xD6\xF0', '\u1F602'),
    rep(NA, 2),
    rep(NA, 2),
    stringsAsFactors=FALSE
  )
  column.classes <- c('logical', 'integer', 'numeric', 'character',
    'raw', 'Date', 'POSIXct_no_time_zone', 'POSIXct_with_time_zone',
    'character', 'list_unnamed', 'list_named')
  column.names <- column.classes
  column.names[length(column.names) - 2] <- '<odd_name>'
  colnames(e) <- column.names
  attr(e[['POSIXct_with_time_zone']], 'tzone') <- 'UTC'
  attr(e[['POSIXct_no_time_zone']], 'tzone') <- test.timezone()
  e[['raw']] <- list(charToRaw('a'), charToRaw('bc'))
  e[['list_unnamed']] <- list(list(1, 2), list())
  e[['list_named']] <- list(
    list(a=1, b=2),
    structure(list(), names=character(0))
  )
  if (!missing(row.indices)) {
    rv <- e[row.indices, , drop=FALSE]
    return(rv)
  }
  return(e)
}

mock_httr_response <- function(
  url,
  status_code,
  state,
  request_body,
  data,
  extra_content,
  next_uri,
  info_uri) {

  if (!missing(extra_content)) {
    content <- extra_content
  } else {
    content <- list()
  }
  content[['stats']] <- list(state=jsonlite::unbox(state))
  content[['id']] <- jsonlite::unbox(gsub('[:/]', '_', url))

  if (!missing(next_uri)) {
    content[['nextUri']] <- jsonlite::unbox(next_uri)
  }

  if (!missing(info_uri)) {
    content[['infoUri']] <- jsonlite::unbox(info_uri)
  }

  if (!missing(data)) {
    presto.types <- lapply(data, function(l) {
      drv <- RPresto::Presto()
      if (is.list(l)) {
        rs.class <- data.class(l[[1]])
        if (rs.class == 'raw') {
          rv <- 'varbinary'
        } else if (rs.class == 'list') {
          rv <- ifelse(is.null(names(l[[1]])), 'array', 'map')
        } else {
          stop('Unsupported mock data type: ', rs.class)
        }
      } else {
        rv <- dbDataType(RPresto::Presto(), l)
      }
      return(rv)
    })

    # Change POSIXct representation, otherwise microseconds
    # are chopped off in toJSON
    old.digits.secs <- options('digits.secs'=3)
    on.exit(options(old.digits.secs), add=TRUE)
    content[['columns']] <- list()
    for (i in seq_along(presto.types)) {
      presto.type <- stringi::stri_trans_tolower(
        presto.types[[i]],
        'en_US.UTF-8'
      )
      content[['columns']][[i]] <- list(
        name=jsonlite::unbox(colnames(data)[i]),
        type=jsonlite::unbox(presto.type),
        typeSignature=list(
          rawType=jsonlite::unbox(presto.type),
          typeArguments=list(),
          literalArguments=list()
        )
      )
      if (presto.type == 'timestamp with time zone') {
        # toJSON ignores the timezone attribute
        data[[i]] <- paste(data[[i]], attr(data[[i]], 'tzone'))
      } else if (presto.type %in% c('array', 'map')) {
        # Lists need to be unboxed
        data[[i]] <- lapply( # Apply to each row
          data[[i]],
          function(l) lapply(l, jsonlite::unbox) # Apply to each item
        )
      }
    }
    content[['data']] <- data
  }

  rv <- list(
    url=url,
    response=structure(
      list(
        url=url,
        status_code=status_code,
        headers=list(
          'content-type'='application/json'
        ),
        content=charToRaw(jsonlite::toJSON(content, dataframe='values'))
      ),
      class='response'
    )
  )
  if (!missing(request_body)) {
    rv[['request_body']] <- request_body
  }
  return(rv)
}

mock_httr_replies <- function(...) {
  response.list <- list(...)
  names(response.list) <- unlist(lapply(response.list, function(l) l[['url']]))
  f <- function(url, body, ...) {
    # Iterate over all specified response mocks and see if both url and body
    # match
    for (i in seq_along(response.list)) {
      item <- response.list[[i]]

      url.matches <- item[['url']] == url
      if(missing(body)) {
        body.matches <- is.null(item[['request_body']])
      } else {
        body.matches <- (
          !is.null(item[['request_body']])
          && item[['request_body']] == body
        )
      }

      if (url.matches && body.matches) {
        return(item[['response']])
      }
    }

    stop(paste0(
      'No mocks for url: ', url,
      if (!missing(body)) {
        paste0(', request_body: ', body)
    }))
  }
  return(f)
}

read_credentials <- function() {
  if (!file.exists('credentials.dcf')) {
    skip(paste0('credential file missing, please create a file ',
         system.file('tests', 'testthat', 'credentials.dcf', package='RPresto'),
         ' with fields "host", "port", "catalog", "schema" to do live testing'
    ))
  }
  dcf <- read.dcf("credentials.dcf")
  credentials <- list(
    host=as.vector(dcf[1, "host"]),
    port=as.integer(as.vector(dcf[1, "port"])),
    catalog=as.vector(dcf[1, "catalog"]),
    schema=as.vector(dcf[1, "schema"]),
    iris_table_name=as.vector(dcf[1, "iris_table_name"])
  )
  return(credentials)
}

setup_live_connection <- function(session.timezone) {
  skip_on_cran()
  credentials <- read_credentials()
  if (missing(session.timezone)) {
    wrapper <- function(expr) suppressWarnings(expr)
    session.timezone <- NULL
  } else {
    wrapper <- function(expr) expr
  }
  conn <- wrapper(dbConnect(RPresto::Presto(),
    schema=credentials$schema,
    catalog=credentials$catalog,
    host=credentials$host,
    port=credentials$port,
    session.timezone=session.timezone,
    user=Sys.getenv('USER')
  ))
  return(conn)
}

setup_live_dplyr_connection <- function(session.timezone) {
  skip_on_cran()

  if(!require('dplyr', quietly=TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }

  credentials <- read_credentials()
  if (missing(session.timezone)) {
    wrapper <- function(expr) suppressWarnings(expr)
    session.timezone <- NULL
  } else {
    wrapper <- function(expr) expr
  }
  db <- wrapper(src_presto(
    RPresto::Presto(),
    schema=credentials$schema,
    catalog=credentials$catalog,
    host=credentials$host,
    port=credentials$port,
    user=Sys.getenv('USER'),
    parameters=list()
  ))
  return(list(db=db, iris_table_name=credentials[['iris_table_name']]))
}

setup_mock_connection <- function() {
  mock.conn <- new('PrestoConnection',
    schema='test',
    catalog='catalog',
    host='http://localhost',
    port=8000L,
    session.timezone=test.timezone(),
    user=Sys.getenv('USER')
  )
  return(mock.conn)
}

setup_mock_dplyr_connection <- function() {
  if(!require('dplyr', quietly=TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  conn <- setup_mock_connection()
  db <- dplyr::src_sql(
    "presto",
    conn,
    info=DBI::dbGetInfo(conn),
    disco=function(x) return(TRUE)
  )

  return(list(db=db, iris_table_name='iris_table'))
}
