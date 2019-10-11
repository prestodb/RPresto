# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoDriver.R
NULL

.presto.to.R <- as.data.frame(matrix(c(
  'boolean', 'logical',
  'bigint', 'integer',
  'integer', 'integer',
  'smallint', 'integer',
  'tinyint', 'integer',
  'decimal', 'character',
  'real', 'numeric',
  'double', 'numeric',
  'varchar', 'character',
  'char', 'character',
  'varbinary', 'raw',
  'json', 'character',
  'date', 'Date',
  'time', 'character',
  'time with time zone', 'character',
  'timestamp', 'POSIXct_no_time_zone',
  'timestamp with time zone', 'POSIXct_with_time_zone',
  'interval year to month', 'character',
  'interval day to second', 'character',
  'array', 'list_unnamed',
  'map', 'list_named',
  'unknown', 'unknown'
), byrow=TRUE, ncol=2), stringsAsFactors=FALSE)
colnames(.presto.to.R) <- c('presto.type', 'R.type')



.R.to.presto <- as.data.frame(matrix(c(
  'boolean', 'logical',
  'bigint', 'integer',
  'double', 'double',
  'varchar', 'character',
  'varbinary', 'raw',
  'date', 'Date',
  'json', NA,
  'time', NA,
  'time with time zone', NA,
  'timestamp', 'POSIXct_no_time_zone',
  'timestamp with time zone', 'POSIXct_with_time_zone',
  'interval year to month', NA,
  'interval day to second', NA,
  'array', 'list_unnamed',
  'map', 'list_named',
  'varchar', 'factor',
  'varchar', 'ordered',
  'varchar', 'NULL'
), byrow=TRUE, ncol=2), stringsAsFactors=FALSE)
colnames(.R.to.presto) <- c('presto.type', 'R.type')

.R.to.presto.env <- new.env(hash=TRUE, size=NROW(.R.to.presto))
for (i in 1:NROW(.R.to.presto)) {
  if (is.na(.R.to.presto[i, 'R.type'])) {
    next
  }
  assign(
    .R.to.presto[i, 'R.type'],
    value=.R.to.presto[i, 'presto.type'],
    envir=.R.to.presto.env
  )
}

.non.complex.types <- c(
  'logical',
  'character',
  'raw',
  'Date',
  'factor',
  'ordered',
  'NULL'
)
.non.complex.types.env <- new.env(hash=TRUE, size=length(.non.complex.types))
for (i in seq_along(.non.complex.types)) {
  assign(.non.complex.types[i], TRUE, envir=.non.complex.types.env)
}


.dbDataType <- function(dbObj, obj, ...) {
  rs.class <- data.class(obj)
  rs.mode <- storage.mode(obj)

  if (!is.null(.non.complex.types.env[[rs.class]])) {
    rv <- .R.to.presto.env[[rs.class]]
  } else if (rs.class == 'numeric') {
    rv <- .R.to.presto.env[[rs.mode]]
  } else if (rs.class == 'POSIXct') {
    tzone <- attr(obj, 'tzone')
    if (is.null(tzone) || tzone == '') {
      index <- 'POSIXct_no_time_zone'
    } else {
      index <- 'POSIXct_with_time_zone'
    }
    rv <- .R.to.presto.env[[index]]
  } else if (rs.class == 'list') {
    if (length(obj) == 0) {
      inner.type <- .dbDataType(dbObj, NULL)
    } else {
      inner.types <- vapply(
        obj,
        function(x) .dbDataType(dbObj, x),
        ''
      )
      inner.type <- inner.types[1]
      if (!all(inner.types == inner.type)) {
        inner.type <- NA
      }
    }
    if (is.na(inner.type)) {
      rv <- 'varchar'
    } else {
      if (!is.null(names(obj))) {
        rv <- paste('map<varchar, ', inner.type, '>', sep='')
      } else {
        rv <- paste('array<', inner.type, '>', sep='')
      }
    }
  } else {
    rv <- 'varchar'
  }

  if (is.na(rv)) {
    rv <- 'varchar'
  }
  # We need to explicitly specify the locale for the upper transformation.
  # For certain locales like tr_TR, the uppercase for 'i' is 'İ'
  # so toupper('bigint') does not give the expected result
  return(stringi::stri_trans_toupper(rv, 'en_US.UTF-8'))
}

#' Return the corresponding presto data type for the given R \code{object}
#' @param dbObj A \code{\linkS4class{PrestoDriver}} object
#' @param obj Any R object
#' @param ... Extra optional parameters, not currently used
#' @return A \code{character} value corresponding to the Presto type for
#'         \code{obj}
#' @rdname dbDataType
#' @details The default value for unknown classes is \sQuote{VARCHAR}.
#'
#' \sQuote{ARRAY}s and \sQuote{MAP}s are supported with some caveats.
#' Unnamed lists will be treated as \sQuote{ARRAY}s and named lists
#' will be a \sQuote{MAP}.
#' All items are expected to be of the same corresponding Presto type,
#' otherwise the default \sQuote{VARCHAR} value is returned.
#' The key type for \sQuote{MAP}s is always \sQuote{VARCHAR}.
#' The \sQuote{value} type for empty lists is always a \sQuote{VARCHAR}.
#'
#' @examples
#' drv <- RPresto::Presto()
#' dbDataType(drv, list())
#' dbDataType(drv, 1)
#' dbDataType(drv, NULL)
#' dbDataType(drv, list(list(list(a=Sys.Date()))))
#' dbDataType(drv, as.POSIXct('2015-03-01 00:00:00', tz='UTC'))
#' dbDataType(drv, Sys.time())
#' # Data types for ARRAY or MAP values can be tricky
#' all.equal('VARCHAR', dbDataType(drv, list(1, 2, 3L)))
#' @export
setMethod('dbDataType', 'PrestoDriver', .dbDataType)
