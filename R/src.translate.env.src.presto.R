# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' S3 implementation of \code{src_translate_env} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#' @export
src_translate_env.src_presto <- function(x) {
  return(dplyr::sql_variant(
    dplyr::sql_translator(.parent = dplyr::base_scalar,
      ifelse = dplyr::sql_prefix("if"),
      as = function(column, type) {
        sql_type <- stringi::stri_trans_toupper(
          dbDataType(Presto(), type),
          'en_US.UTF-8'
        )
        dplyr::build_sql('CAST(', column, ' AS ', dplyr::ident(sql_type), ')')
      },
      tolower = dplyr::sql_prefix("lower"),
      toupper = dplyr::sql_prefix("upper"),
      pmax = dplyr::sql_prefix("greatest"),
      pmin = dplyr::sql_prefix("least"),
      is.finite = dplyr::sql_prefix("is_finite"),
      is.infinite = dplyr::sql_prefix("is_infinite"),
      is.nan = dplyr::sql_prefix("is_nan")
    ),
    dplyr::sql_translator(.parent = dplyr::base_agg,
      n = function() dplyr::sql("count(*)"),
      sd =  dplyr::sql_prefix("stddev_samp"),
      var = dplyr::sql_prefix("var_samp"),
      all = dplyr::sql_prefix("bool_and"),
      any = dplyr::sql_prefix("bool_or")
    ),
    dplyr::base_win
  ))
}
