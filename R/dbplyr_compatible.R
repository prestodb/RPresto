# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# Some functions have moved to a separate package called dbplyr for the
# dplyr 0.6.0 release. This function allows us to provide backward
# compatibility by importing from dplyr when necessary.
dbplyr_compatible <- function(function_name) {
  if (utils::packageVersion('dplyr') >= '0.5.0.9004') {
    if (!requireNamespace('dbplyr', quietly=TRUE)) {
      stop(function_name, ' requires the dbplyr package, please install it ',
        'first and try again'
      )
    }
    f <- utils::getFromNamespace(function_name, 'dbplyr')
  } else {
    f <- utils::getFromNamespace(function_name, 'dplyr')
  }
  return(f)
}
