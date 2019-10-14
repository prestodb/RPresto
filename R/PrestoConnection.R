# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include PrestoSession.R
NULL

#' S4 implementation of \code{DBIConnection} for Presto.
#'
#' @keywords internal
#' @export
setClass('PrestoConnection',
  contains='DBIConnection',
  slots=c(
    'catalog'='character',
    'schema'='character',
    'user'='character',
    'host'='character',
    'port'='integer',
    'source'='character',
    'session.timezone'='character',
    'Id'='character',
    'session'='PrestoSession'
  )
)

#' @rdname PrestoConnection-class
#' @export
setMethod('show',
  'PrestoConnection',
  function(object) {
    cat(
      '<PrestoConnection: ', object@host, ':', object@port, '>\n',
      'Catalog: ', object@catalog, '\n',
      'Schema: ', object@schema, '\n',
      'User: ', object@user, '\n',
      'Source: ', object@source, '\n',
      'Session Time Zone: ', object@session.timezone, '\n',
      sep=''
    )
    parameters <- object@session$parameters()
    if (!is.null(parameters) && length(parameters)) {
      cat(
        'Parameters:\n',
        paste(
          '\t', names(parameters), ': ', unlist(parameters), '\n',
          sep='',
          collapse=''
        ),
        sep=''
      )
    }
  }
)
