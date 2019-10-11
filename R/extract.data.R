# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' @include json.tabular.to.data.frame.R dbDataType.R
NULL

.extract.data <- function(response.content, timezone) {
  data.list <- response.content[['data']]
  if (is.null(data.list)) {
    data.list <- list()
  }
  column.list <- response.content[['columns']]
  if (is.null(column.list)) {
    column.list <- list()
  }
  column.info <- .json.tabular.to.data.frame(
    column.list,
    # name, type, typeSignature
    c(varchar='character', varchar='character', map='list_named')
  )
  # The typeSignature item for each column has a 'rawType' value which
  # corresponds to the Presto data type.
  presto.types <- vapply(
    column.info[['typeSignature']],
    function(x) x[['rawType']],
    ''
  )
  r.types <- with(.presto.to.R, R.type[match(presto.types, presto.type)])
  names(r.types) <- presto.types
  rv <- .json.tabular.to.data.frame(data.list, r.types, timezone=timezone)
  if (!is.null(column.info[['name']])) {
    colnames(rv) <- column.info[['name']]
  }
  return(rv)
}
