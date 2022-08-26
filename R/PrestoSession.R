# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' Internal implementation detail class needed for its side-effects.
#' When SET/RESET SESSION queries are called, session parameters need to be
#' maintained by the client and requires an in-place update.
PrestoSession <- setRefClass('PrestoSession',
  fields=c(
    '.parameters'
  ),
  methods=list(
    initialize=function(parameters, ...) {
      initFields(.parameters = parameters)
    },
    setParameter=function(key, value) {
      .parameters[[key]] <<- value
    },
    unsetParameter=function(key) {
      .parameters[[key]] <<- NULL
    },
    parameters=function() {
      return(.parameters)
    },
    parameterString=function() {
      return(paste(
        names(.parameters),
        .parameters,
        sep='=',
        collapse=','
      ))
    }
  )
)
