# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

get_tables_from_sql <- function(query) {
  UseMethod("get_tables_from_sql")
}

#' @export
get_tables_from_sql.lazy_select_query <- function(query) {
  get_tables_from_sql(query$x)
}

#' @export
get_tables_from_sql.lazy_base_remote_query <- function(query) {
  as.character(query$x)
}

#' @export
get_tables_from_sql.lazy_join_query <- function(query) {
  c(get_tables_from_sql(query$x), get_tables_from_sql(query$y))
}

#' @export
get_tables_from_sql.lazy_semi_join_query <- function(query) {
  c(get_tables_from_sql(query$x), get_tables_from_sql(query$y))
}

#' @export
get_tables_from_sql.lazy_set_op_query <- function(query) {
  c(get_tables_from_sql(query$x), get_tables_from_sql(query$y))
}

is_cte_used <- function(sql) {
  startsWith(tolower(sql), "with")
}

find_cte_tables_from_lazy_query <- function(con, sql, ...) {
  stopifnot(inherits(sql, "lazy_query"))
  from_tables <- unique(get_tables_from_sql(sql))
  cte_tables <- con@session$getCTENames()
  matching_cte_tables <- intersect(cte_tables, from_tables)
  if (length(matching_cte_tables) > 0) {
    return(find_recursive_cte_tables(con, list(matching_cte_tables)))
  } else {
    return(matching_cte_tables)
  }
}

find_recursive_cte_tables <- function(con, cte_tables_list, ...) {
  stopifnot(is.list(cte_tables_list))
  level <- length(cte_tables_list)
  dependent_tables <- unique(
    purrr::flatten_chr(
      purrr::map(
        cte_tables_list[[level]], con@session$findDependentCTEs
      )
    )
  )
  dependent_ctes <- intersect(con@session$getCTENames(), dependent_tables)
  if (length(dependent_ctes) > 0) {
    cte_tables_list[[level + 1]] <- dependent_ctes
    find_recursive_cte_tables(con, cte_tables_list, ...)
  } else {
    return(unique(unlist(rev(cte_tables_list))))
  }
}

find_cte_tables_from_try_result <- function(con, res, ...) {
  if (inherits(res, "try-error")) {
    regex_pattern <- ".* Table (.+) does not exist"
    error_msg <- conditionMessage(attr(res, "condition"))
    if (grepl(regex_pattern, error_msg)) {
      missing_table <-
        stringi::stri_match_all_regex(error_msg, regex_pattern)[[1]][2]
      missing_table_name <- utils::tail(strsplit(missing_table, "\\.")[[1]], 1)
      if (con@session$hasCTE(missing_table_name)) {
        return(missing_table_name)
      } else {
        return(c())
      }
    } else {
      return(c())
    }
  } else {
    return(c())
  }
}

build_cte_query <- function(con, name, query, ...) {
  dbplyr::build_sql(
    dbplyr::ident(name), dbplyr::sql(" AS"), " (\n", dbplyr::sql(query), "\n)",
    con = con,
    ...
  )
}

generate_sql_with_cte <- function(con, statement, cte_tables, ...) {
  stopifnot(length(cte_tables) > 0)
  # stopifnot(startsWith(tolower(statement), "select"))
  query_list <- con@session$getCTEs(cte_tables)
  # Adapted from dbplyr:::cte_render()
  # (see https://github.com/cran/dbplyr/blob/master/R/sql-build.R#L86)
  cte_queries <- purrr::imap(query_list, ~ build_cte_query(con, .y, .x))
  cte_query <- dbplyr::sql_vector(
    unname(cte_queries),
    parens = FALSE,
    collapse = ",\n",
    con = con
  )
  dbplyr::build_sql(
    dbplyr::sql("WITH "), cte_query, "\n", dbplyr::sql(statement),
    con = con
  )
}
