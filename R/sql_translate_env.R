# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

#' @include dbplyr_compatible.R PrestoConnection.R
NULL

presto_window_functions <- function() {
  base_win <- dbplyr_compatible('base_win')
  if (utils::packageVersion('dplyr') < '0.5.0.9004') {
    return(base_win)
  }
  sql_translator <- dbplyr_compatible('sql_translator')
  win_absent <- dbplyr_compatible('win_absent')
  win_recycled <- dbplyr_compatible('win_recycled')
  return(sql_translator(
    .parent=base_win,
    all=win_recycled('bool_and'),
    any=win_recycled('bool_or'),
    n_distinct=win_absent('n_distinct'),
    sd=win_recycled("stddev_samp")
  ))
}

#' S3 implementation of \code{sql_translate_env} for Presto.
#'
#' @rdname dplyr_function_implementations
#' @keywords internal
#'
#' @rawNamespace
#' if (getRversion() >= "3.6.0") {
#'   S3method(dplyr::sql_translate_env,PrestoConnection)
#' } else {
#'   export(sql_translate_env.PrestoConnection)
#' }
sql_translate_env.PrestoConnection <- function(con) {
  sql_variant <- dbplyr_compatible('sql_variant')
  sql_translator <- dbplyr_compatible('sql_translator')
  sql_prefix <- dbplyr_compatible('sql_prefix')
  sql_cast <- dbplyr_compatible('sql_cast')
  sql <- dbplyr_compatible('sql')
  build_sql <- dbplyr_compatible('build_sql')
  base_scalar <- dbplyr_compatible('base_scalar')
  base_agg <- dbplyr_compatible('base_agg')
  return(sql_variant(
    sql_translator(.parent = base_scalar,
      ifelse = sql_prefix("IF"),
      as = function(column, type) {
        sql_type <- stringi::stri_trans_toupper(
          dbDataType(Presto(), type),
          'en_US.UTF-8'
        )
        build_sql('CAST(', column, ' AS ', sql(sql_type), ')')
      },
      as.character = sql_cast("VARCHAR"),
      as.numeric = sql_cast("DOUBLE"),
      as.double = sql_cast("DOUBLE"),
      as.integer = sql_cast("BIGINT"),
      as.Date = sql_cast("DATE"),
      as.logical = sql_cast("BOOLEAN"),
      as.raw = sql_cast("VARBINARY"),
      tolower = sql_prefix("LOWER"),
      toupper = sql_prefix("UPPER"),
      pmax = sql_prefix("GREATEST"),
      pmin = sql_prefix("LEAST"),
      is.finite = sql_prefix("IS_FINITE"),
      is.infinite = sql_prefix("IS_FINITE"),
      is.nan = sql_prefix("IS_NAN")
    ),
    sql_translator(.parent = base_agg,
      n = function() sql("COUNT(*)"),
      sd =  sql_prefix("STDDEV_SAMP"),
      var = sql_prefix("VAR_SAMP"),
      all = sql_prefix("BOOL_AND"),
      any = sql_prefix("BOOL_OR")
    ),
    presto_window_functions()
  ))
}
