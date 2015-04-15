# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include utility_functions.R
NULL

#' Internal implementation detail class needed for its side-effects.
#' When dbFetch is called, we need to both return the data and update
#' the uri to the next value.
PrestoCursor <- setRefClass('PrestoCursor',
  fields=c(
    '.uri'='character',
    '.info.uri'='character',
    '.state'='character',
    '.fetched.row.count'='integer',
    '.stats'='list'
  ),
  methods=list(
    initialize=function(post.content, ...) {
      initFields(.fetched.row.count=as.integer(0))
      updateCursor(post.content, 0)
    },
    updateCursor=function(response.content, row.count) {

      if (is.null(response.content[['nextUri']])) {
        .uri <<- ""
      } else {
        .uri <<- response.content[['nextUri']]
      }

      if (is.null(response.content[['infoUri']])) {
        .info.uri <<- ""
      } else {
        .info.uri <<- response.content[['infoUri']]
      }

      state(get.state(response.content))

      stats(response.content[['stats']])

      .fetched.row.count <<- .fetched.row.count + as.integer(row.count)
    },
    infoUri=function() {
      return(.info.uri)
    },
    nextUri=function() {
      return(.uri)
    },
    fetchedRowCount=function() {
      return(.fetched.row.count)
    },
    state=function(new.state) {
      old.state <- .state
      if (!missing(new.state)) {
        if (is.null(new.state)) {
          .state <<- ""
        } else {
          .state <<- new.state
        }
      }
      return(old.state)
    },
    stats=function(new.stats) {
      old.stats <- .stats
      if (!missing(new.stats)) {
        .stats <<- new.stats
      }
      return(old.stats)
    },
    hasCompleted=function() {
      return(.uri == '')
    }
  )
)
