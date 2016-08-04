# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('.json.tabular.to.data.frame')

source('utilities.R')

test_that('edge cases are handled correctly', {
    .json.tabular.to.data.frame <- RPresto:::.json.tabular.to.data.frame
    expect_equal_data_frame(
      .json.tabular.to.data.frame(list(), character(0)),
      data.frame()
    )
    expect_error(
      .json.tabular.to.data.frame(1, 'a'),
      'Unexpected data class',
      label='Unexpected data class'
    )
    expect_error(
      .json.tabular.to.data.frame(list(list(1)), 'a'),
      'Unsupported column type',
      label='Unsupported column type'
    )
    expect_warning(
      .json.tabular.to.data.frame(
        list(
          list(a=1L),
          list(b=1L)
        ),
        'integer'
      ),
      'Item .*, column names differ across rows',
      label='Different column names'
    )
    expect_error(
      .json.tabular.to.data.frame(list(list(a=1)), c('integer', 'raw')),
      'Item .*,.+expected: 2 columns,.+received: 1',
      label='Not enough columns'
    )
    e <- data.frame(
      logical=TRUE,
      integer=1L,
      numeric=0.0,
      character='',
      Date=as.Date('2014-03-01'),
      POSIXct_no_time_zone=as.POSIXct(
        '2015-03-01 12:00:00',
        tz=test.timezone()
      ),
      POSIXct_with_time_zone=as.POSIXct('2015-03-01 12:00:00', tz='UTC'),
      stringsAsFactors=FALSE)
    e[['list_unnamed']] <- list(list(1))
    e[['list_named']] <- list(list(a=1))
    e[['raw']] <- list(charToRaw('abc'))
    attr(e[['POSIXct_with_time_zone']], 'tzone') <- NULL

    column.types <- c(
        'logical', 'integer', 'numeric', 'character', 'Date',
        'POSIXct_no_time_zone', 'POSIXct_with_time_zone',
        'list_unnamed', 'list_named', 'raw'
    )
    r <- .json.tabular.to.data.frame(
      list(),
      column.types,
      timezone=test.timezone()
    )
    colnames(r) <- column.types

    expect_equal_data_frame(r, e[FALSE, ])

    r <- .json.tabular.to.data.frame(
      rep(list(list()), 3),
      character(0),
      timezone=test.timezone()
    )
    expect_equal_data_frame(r, data.frame(rep(NA, 3))[, FALSE, drop=FALSE])
})

with_locale(test.locale(), test_that)('regular data is converted correctly', {
  .json.tabular.to.data.frame <- RPresto:::.json.tabular.to.data.frame

  input <- list(
    list(
      TRUE,
      1L,
      0.0,
      '0',
      '',
      'YQ==', # a
      '2015-03-01',
      '2015-03-01 12:00:00',
      '2015-03-01 12:00:00 Europe/Paris',
      iconv('\xFD\xDD\xD6\xF0', localeToCharset(test.locale()), 'UTF-8'),
      list(1, 2),
      list(a=1, b=2)
    ),
    list(
      FALSE,
      2L,
      1.0,
      '1.414',
      'z',
      'YmM=', # bc
      '2015-03-02',
      '2015-03-02 12:00:00.321',
      '2015-03-02 12:00:00.321 Europe/Paris',
      { x <- '\xE1\xBD\xA0\x32'; Encoding(x) <- 'UTF-8'; x},
      list(),
      structure(list(), names=character(0))
    )
  )

  column.classes <- c('logical', 'integer', 'numeric', 'character', 'character',
    'raw', 'Date', 'POSIXct_no_time_zone', 'POSIXct_with_time_zone',
    'character', 'list_unnamed', 'list_named')
  column.names <- column.classes
  column.names[length(column.names) - 2] <- '<odd_name>'
  e <- data.frame.with.all.classes()

  r <- .json.tabular.to.data.frame(
    input,
    column.classes,
    timezone=test.timezone()
  )
  colnames(r) <- column.names

  expect_equal_data_frame(r, e, label='unnamed items')

  old.locale <- Sys.getlocale('LC_CTYPE')
  tryCatch({
      if (.Platform[['OS.type']] == 'windows') {
        Sys.setlocale('LC_CTYPE', 'French_France.1252')
      } else {
        Sys.setlocale('LC_CTYPE', 'fr_FR.iso8859-15@euro')
      }
    },
    warning=function(cond) {
      Sys.setlocale('LC_CTYPE', 'fr_FR.iso8859-15')
    }
  )
  if (Sys.getlocale('LC_CTYPE') != old.locale) {
    # This will fail because data.frame.with.all.classes() returns
    # the first item of '<odd_name>' without an explicit encoding.
    # However the data given to json.tabular.to.data.frame is reencoded
    # to utf-8 from the test encoding which is not iso8859-15.
    # Therefore, the comparison is effectively between:
    # iconv('\xFD...', 'iso8859-15', 'utf8')
    # iconv('\xFD...', '<test_encoding>', 'utf8')
    expect_false(isTRUE(all.equal(r, e)))
  }

  input.with.names <- lapply(input,
    function(x) { names(x) <- column.names; return(x) }
  )
  Sys.setlocale('LC_CTYPE', test.locale())
  r <- .json.tabular.to.data.frame(
    input.with.names,
    column.classes,
    timezone=test.timezone()
  )
  expect_equal_data_frame(r, e, label='auto parse names')
})

test_that('NAs are handled correctly', {
  .json.tabular.to.data.frame <- RPresto:::.json.tabular.to.data.frame
  expect_equal_data_frame(
    .json.tabular.to.data.frame(list(list(A=NULL)), 'logical'),
    data.frame(A=NA)
  )

  e <- data.frame(A=as.Date(NA), B=3L, C=as.POSIXct(NA))
  attr(e[['C']], 'tzone') <- NULL
  expect_equal_data_frame(
    .json.tabular.to.data.frame(
      list(list(A=NULL, B=3L, C=NULL)),
      c('Date', 'integer', 'POSIXct_with_time_zone'),
      timezone=test.timezone()
    ),
    e
  )

  column.classes <- c('logical', 'integer', 'numeric', 'character',
    'raw', 'Date', 'POSIXct_no_time_zone', 'POSIXct_with_time_zone',
    'list_unnamed', 'list_named')

  r <- .json.tabular.to.data.frame(
    list(rep(list(NULL), length(column.classes))),
    column.classes,
    timezone=test.timezone()
  )
  colnames(r) <- column.classes

  e <- data.frame(NA, NA_integer_, NA_real_, NA_character_, NA,
    as.Date(NA), as.POSIXct(NA_character_), as.POSIXct(NA_character_),
    NA, NA, stringsAsFactors=FALSE)
  colnames(e) <- column.classes
  e[['raw']] <- list(NA)
  e[['list_unnamed']] <- list(NA)
  e[['list_named']] <- list(NA)
  attr(e[['POSIXct_no_time_zone']], 'tzone') <- test.timezone()
  attr(e[['POSIXct_with_time_zone']], 'tzone') <- NULL

  expect_equal_data_frame(r, e)

  input <- list(
    list(
      logical=NULL,
      integer=1L,
      numeric=NULL,
      character='',
      raw=NULL,
      Date='2015-03-01',
      POSIXct_no_time_zone=NULL,
      POSIXct_with_time_zone='2015-04-01 01:02:03.456 Europe/Paris',
      list_unnamed=NULL,
      list_named=list(A=1)
    ),
    list(
      logical=TRUE,
      integer=NULL,
      numeric=0.0,
      character=NULL,
      raw='YQ==',
      Date=NULL,
      POSIXct_no_time_zone='2015-04-01 01:02:03.456',
      POSIXct_with_time_zone=NULL,
      list_unnamed=list(1),
      list_named=NULL
    )
  )

  e <- data.frame(
    logical=c(NA, TRUE),
    integer=c(1L, NA),
    numeric=c(NA, 0.0),
    character=c('', NA),
    raw=NA,
    Date=as.Date(c('2015-03-01', NA)),
    POSIXct_no_time_zone
      =as.POSIXct(c(NA, '2015-04-01 01:02:03.456'), tz=test.timezone()),
    POSIXct_with_time_zone=as.POSIXct(
      c('2015-04-01 01:02:03.456', NA),
      tz='Europe/Paris'
    ),
    list_unnamed=NA,
    list_named=NA,
    stringsAsFactors=FALSE
  )
  e[['raw']] <- list(NA, charToRaw('a'))
  e[['list_unnamed']] <- list(NA, list(1))
  e[['list_named']] <- list(list(A=1), NA)

  r <- .json.tabular.to.data.frame(
    input,
    column.classes,
    timezone=test.timezone()
  )
  expect_equal_data_frame(r, e)

  e.reversed <- e[c(2, 1), ]
  rownames(e.reversed) <- NULL
  r <- .json.tabular.to.data.frame(
    input[c(2, 1)],
    column.classes,
    timezone=test.timezone()
  )
  expect_equal_data_frame(r, e.reversed)
})

test_that('Inf, -Inf and NaN are handled correctly', {
  .json.tabular.to.data.frame <- RPresto:::.json.tabular.to.data.frame
  expect_equal_data_frame(
    .json.tabular.to.data.frame(
      list(list(A='Infinity', B='-Infinity', C='NaN')),
      c('numeric', 'numeric', 'numeric')
    ),
    data.frame(A=Inf, B=-Inf, C=NaN)
  )

  expect_equal_data_frame(
    .json.tabular.to.data.frame(
      list(list(A='Infinity', B='-Infinity', C='NaN')),
      c('character', 'character', 'character')
    ),
    data.frame(A='Infinity', B='-Infinity', C='NaN', stringsAsFactors=FALSE)
  )

  expect_equal(
    .json.tabular.to.data.frame(
      list(
        list(A=1.0, B=1.0, C=1.0),
        list(A='Infinity', B='-Infinity', C='NaN'),
        list(A=1.0, B=1.0, C=1.0)
      ),
      c('numeric', 'numeric', 'numeric')
    ),
    data.frame(A=c(1.0, Inf, 1.0), B=c(1.0, -Inf, 1.0), C=c(1.0, NaN, 1.0))
  )

})
