# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# A database connection might not be available in some environments (e.g.
# Github check-standard actions, CRAN rebuilding vignettes, etc). Telling if
# a basic data connection (we use a database in memory) is available thus is
# useful to adjust the code accordingly. This file is heavily inspired by the
# same approach from the RPostgres package.

connect_default <- function(...) {
  DBI::dbConnect(
    drv = Presto(),
    host = "http://localhost",
    port = 8080,
    user = Sys.getenv("USER"),
    catalog = "memory",
    schema = "default",
    ...
  )
}

#' @description
#' `presto_default()` works similarly but returns a connection on success and
#' throws a testthat skip condition on failure, making it suitable for use in
#' tests.
#' @export
#' @rdname presto_has_default
presto_default <- function(...) {
  tryCatch(
    {
      connect_default(...)
    },
    error = function(...) {
      testthat::skip("Test database not available")
    })
}

#' Check if default database is available.
#'
#' RPresto examples and tests connect to a default database via
#' `dbConnect(Presto(), ...)`. This function checks if that
#' database is available, and if not, displays an informative message.
#'
#' @param ... Additional arguments passed on to [DBI::dbConnect()]
#' @export
#' @examples
#' if (presto_has_default()) {
#'   db <- presto_default()
#'   print(dbListTables(db))
#'   dbDisconnect(db)
#' } else {
#'   message("No database connection.")
#' }
presto_has_default <- function(...) {
  tryCatch(
    {
      con <- connect_default(...)
      DBI::dbDisconnect(con)
      TRUE
    },
    error = function(...) {
      message(
        "Could not initialise default Presto database. If Presto is\n",
        "running, check the memory connector is set up correctly."
      )
      FALSE
    })
}
