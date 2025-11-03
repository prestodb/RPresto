# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "bigint overflow warning with column name"))

test_that("BIGINT to integer overflow warning includes column name", {
  conn <- setup_live_connection()

  expect_warning(
    {
      res <- dbGetQuery(
        conn,
        "SELECT CAST(POW(2, 31) AS BIGINT) AS bigint_overflow",
        bigint = "integer"
      )
      invisible(res)
    },
    regexp = "BIGINT to integer overflow in column 'bigint_overflow'",
    fixed = FALSE
  )
})

test_that("Multiple BIGINT columns emit separate column-aware warnings", {
  conn <- setup_live_connection()

  warnings <- character(0)
  handler <- function(w) {
    warnings <<- c(warnings, conditionMessage(w))
    invokeRestart("muffleWarning")
  }

  sql <- paste0(
    "SELECT CAST(POW(2, 31) AS BIGINT) AS a, ",
    "CAST(-POW(2, 31) - 1 AS BIGINT) AS b"
  )

  withCallingHandlers(
    dbGetQuery(conn, sql, bigint = "integer"),
    warning = handler
  )

  expect_true(any(grepl("in column 'a'", warnings, fixed = TRUE)))
  expect_true(any(grepl("in column 'b'", warnings, fixed = TRUE)))
})

test_that("Multiple rows with BIGINT overflow report correct column once", {
  conn <- setup_live_connection()

  warnings <- character(0)
  handler <- function(w) {
    warnings <<- c(warnings, conditionMessage(w))
    invokeRestart("muffleWarning")
  }

  sql <- paste0(
    "SELECT CAST(POW(2, 31) AS BIGINT) AS a ",
    "UNION ALL ",
    "SELECT CAST(POW(2, 31) + 1 AS BIGINT) AS a"
  )

  withCallingHandlers(
    dbGetQuery(conn, sql, bigint = "integer"),
    warning = handler
  )

  # Expect at least one column-aware warning mentioning column 'a'
  expect_true(any(grepl("in column 'a'", warnings, fixed = TRUE)))
})

test_that(
  paste(
    "2-row table with BIGINT and ARRAY(ROW) overflows emits column-aware",
    "warnings for both columns"
  ),
  {
    conn <- setup_live_connection()

    warnings <- character(0)
    handler <- function(w) {
      warnings <<- c(warnings, conditionMessage(w))
      invokeRestart("muffleWarning")
    }

    row_type <- "ROW(id BIGINT, name VARCHAR)"
    arr_cast <- paste0("AS ARRAY(", row_type, ")")

    row1_arr <- paste0(
      "ARRAY[",
      "CAST(ROW(CAST(POW(2, 31) AS BIGINT), 'a') AS ", row_type, "),",
      "CAST(ROW(CAST(POW(2, 31) + 1 AS BIGINT), 'b') AS ", row_type, ")",
      "]"
    )
    row2_arr <- paste0(
      "ARRAY[",
      "CAST(ROW(CAST(POW(2, 31) AS BIGINT), 'x') AS ", row_type, "),",
      "CAST(ROW(CAST(POW(2, 31) AS BIGINT), 'y') AS ", row_type, "),",
      "CAST(ROW(CAST(1 AS BIGINT), 'z') AS ", row_type, ")",
      "]"
    )

    sql <- paste0(
      "SELECT CAST(1 AS BIGINT) AS simple, ",
      "CAST(", row1_arr, " ", arr_cast, ") AS arr ",
      "UNION ALL ",
      "SELECT CAST(POW(2, 31) AS BIGINT) AS simple, ",
      "CAST(", row2_arr, " ", arr_cast, ") AS arr"
    )

    withCallingHandlers(
      dbGetQuery(conn, sql, bigint = "integer"),
      warning = handler
    )

    expect_true(any(grepl("in column 'simple'", warnings, fixed = TRUE)))
    expect_true(any(grepl("in column 'arr.id'", warnings, fixed = TRUE)))
  }
)

test_that("Overflow warning can be suppressed via option", {
  conn <- setup_live_connection()

  local_options <- list(rpresto.bigint_overflow.warning = FALSE)
  old <- options(local_options)
  on.exit(options(old), add = TRUE)

  expect_silent(
    dbGetQuery(
      conn,
      "SELECT CAST(POW(2, 31) AS BIGINT) AS bigint_overflow",
      bigint = "integer"
    )
  )
})

test_that(
  "Overflow warning for ARRAY<ROW<id BIGINT, name VARCHAR>> mentions column name",
  {
  conn <- setup_live_connection()

  # Construct an array of rows where id contains an out-of-range BIGINT
  # Expect a warning on coercion to integer, even though the BIGINT is nested
  expect_warning(
    {
      sql <- paste0(
        "SELECT CAST(ARRAY[",
        "CAST(ROW(CAST(POW(2, 31) AS BIGINT), 'too_big') ",
        "AS ROW(id BIGINT, name VARCHAR))] ",
        "AS ARRAY(ROW(id BIGINT, name VARCHAR))) AS arr"
      )
      res <- dbGetQuery(
        conn,
        sql,
        bigint = "integer"
      )
      invisible(res)
    },
    regexp = "in column 'arr.id'",
    fixed = FALSE
  )
})

