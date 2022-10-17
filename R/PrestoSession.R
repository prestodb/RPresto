# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

#' Class to encapsulate a Presto session
#'
#' A session contains temporary attributes and information that are only useful
#' for the session. It's attached to a PrestoConnection for as long as as the
#' connection lives. There are a few types of information stored.
#'   1) Session properties that can be set via query response headers and need
#'      to be sent with following HTTP requests.
#'   2) Common table expressions (CTEs) that can be used to store a subquery and
#'      be used in a WITH statement.
#'
#' @slot .parameters List of Presto session parameters to be added to the
#'         X-Presto-Session header.
#' @slot .ctes List of common table expressions (CTEs), i.e. SELECT statements
#'         with names. They can be used in a WITH statement.
#' @keywords internal
PrestoSession <- setRefClass("PrestoSession",
  fields = c(
    ".parameters",
    ".ctes"
  ),
  methods = list(
    initialize = function(parameters, ctes = list(), ...) {
      initFields(.parameters = parameters, .ctes = list())
      if (length(ctes) > 0) {
        if ("" %in% names(ctes)) {
          stop("All CTEs are not named.", call. = FALSE)
        }
        for (i in seq_along(ctes)) {
          addCTE(name = names(ctes)[i], sql = ctes[[i]], replace = TRUE)
        }
      }
    },
    # Parameter methods
    setParameter = function(key, value) {
      .parameters[[key]] <<- value
    },
    unsetParameter = function(key) {
      .parameters[[key]] <<- NULL
    },
    parameters = function() {
      return(.parameters)
    },
    parameterString = function() {
      return(paste(
        names(.parameters),
        .parameters,
        sep = "=",
        collapse = ","
      ))
    },
    # CTE methods
    getCTENames = function() {
      names(.ctes)
    },
    hasCTE = function(name) {
      name %in% getCTENames()
    },
    addCTE = function(name, sql, replace = FALSE) {
      if (!is.character(sql)) {
        stop("CTE ", name, " body is not character.", call. = FALSE)
      }
      if (hasCTE(name)) {
        if (identical(replace, TRUE)) {
          .ctes[[match(name, getCTENames())]] <<- sql
          message("CTE ", name, " is replaced.")
        } else {
          stop(
            "CTE ", name, " already exists and repalce is set to FALSE.",
            call. = FALSE
          )
        }
      } else {
        .ctes[[name]] <<- dbplyr::sql(sql)
      }
      invisible(TRUE)
    },
    removeCTE = function(name) {
      if (hasCTE(name)) {
        .ctes[[name]] <<- NULL
      }
      invisible(TRUE)
    },
    getCTEs = function(names = NULL) {
      if (is.null(names)) {
        return(.ctes)
      }
      exist_ctes <- names %in% getCTENames()
      if (!all(exist_ctes)) {
        na_ctes <- paste(names[!exist_ctes], collapse = ",")
        stop("CTEs [", na_ctes, "] don't exist.", call. = FALSE)
      }
      return(.ctes[names])
    },
    retrieveCTE = function(name) {
      if (hasCTE(name)) {
        return(.ctes[[name]])
      } else {
        stop("CTE ", name, " does not exist.", call. = FALSE)
      }
    },
    findDependentCTEs = function(name) {
      dependent_ctes <- unique(
        stringi::stri_extract_all_regex(
          str = retrieveCTE(name),
          pattern = "(?<=[FROM|JOIN] \").+?(?=\")"
        )[[1]]
      )
      if (length(dependent_ctes) == 1L && is.na(dependent_ctes)) {
        return(c())
      } else {
        return(dependent_ctes)
      }
    }
  )
)
