# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

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
    'session.timezone'='character',
    'Id'='character',
    'parameters'='list'
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
      'Session Time Zone: ', object@session.timezone, '\n',
      sep=''
    )
    parameters <- object@parameters
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
