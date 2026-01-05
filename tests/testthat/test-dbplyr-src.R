# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context(paste(Sys.getenv("PRESTO_TYPE", "Presto"), "dbplyr-src compute"))

test_that("compute() waits for table existence - immediate success", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  
  parts <- setup_mock_dplyr_connection()
  db <- parts[["db"]]
  
  # Track HTTP POST calls for dbExistsTable queries
  dbexists_post_calls <- 0
  
  # Create a wrapper that tracks dbExistsTable calls
  create_mock_httr_post <- function() {
    base_mock <- mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SHOW COLUMNS FROM iris",
        data = data.frame(
          Column = c("sepal.length", "sepal.width", "petal.length", "petal.width", "species"),
          stringsAsFactors = FALSE
        )
      ),
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "^CREATE TABLE",
        data = data.frame()
      )
    )
    
    function(url, body, ...) {
      # Track dbExistsTable queries
      if (grepl("SELECT COUNT\\(\\*\\) AS n.*FROM information_schema.columns", body)) {
        dbexists_post_calls <<- dbexists_post_calls + 1
        # First call is from db_save_query (should return FALSE - table doesn't exist yet)
        # Second call is from our retry logic (should return TRUE - table exists)
        if (dbexists_post_calls == 1) {
          return(mock_httr_response(
            url,
            status_code = 200,
            state = "FINISHED",
            request_body = body,
            data = data.frame(n = 0, stringsAsFactors = FALSE)
          )[["response"]])
        }
        # Second call: table exists
        return(mock_httr_response(
          url,
          status_code = 200,
          state = "FINISHED",
          request_body = body,
          data = data.frame(n = 1, stringsAsFactors = FALSE)
        )[["response"]])
      }
      return(base_mock(url, body, ...))
    }
  }
  
  with_mocked_bindings(
    {
      # Create a mock lazy query inside the mocked bindings
      iris_presto <- dplyr::tbl(db, "iris")
      iris_summary <- iris_presto %>%
        dplyr::group_by(species) %>%
        dplyr::summarise(mean_sepal = mean(sepal.length))
      
      result <- dplyr::compute(iris_summary, "test_table", cte = FALSE)
      expect_is(result, "tbl_presto")
    },
    # Mock dbListFields to avoid HTTP calls when creating tbl
    dbListFields = function(conn, name, ...) {
      c("sepal.length", "sepal.width", "petal.length", "petal.width", "species")
    },
    # Mock db_sql_render to return SQL string
    db_sql_render = function(con, sql, ...) {
      dbplyr::sql("SELECT 1")
    },
    httr_POST = create_mock_httr_post(),
    httr_GET = mock_httr_replies()
  )
  
  # Verify dbExistsTable was called 3 times:
  # 1 from db_save_query (to check before creating)
  # 2 from our retry logic (while condition check + after-loop check)
  # For immediate success, the while condition finds the table exists, so loop doesn't execute
  expect_equal(dbexists_post_calls, 3)
})

test_that("compute() waits for table existence - retry needed", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  
  parts <- setup_mock_dplyr_connection()
  db <- parts[["db"]]
  
  dbexists_post_calls <- 0
  
  create_mock_httr_post <- function() {
    function(url, body, ...) {
      # Track dbExistsTable queries
      if (grepl("SELECT COUNT\\(\\*\\) AS n.*FROM information_schema.columns", body)) {
        dbexists_post_calls <<- dbexists_post_calls + 1
        # First call is from db_save_query (should return FALSE - table doesn't exist yet)
        if (dbexists_post_calls == 1) {
          return(mock_httr_response(
            url,
            status_code = 200,
            state = "FINISHED",
            request_body = body,
            data = data.frame(n = 0, stringsAsFactors = FALSE)
          )[["response"]])
        }
        # Second call is from our retry logic - first check (doesn't exist)
        if (dbexists_post_calls == 2) {
          return(mock_httr_response(
            url,
            status_code = 200,
            state = "FINISHED",
            request_body = body,
            data = data.frame(n = 0, stringsAsFactors = FALSE)
          )[["response"]])
        }
        # Third call is from our retry logic - second check (exists)
        return(mock_httr_response(
          url,
          status_code = 200,
          state = "FINISHED",
          request_body = body,
          data = data.frame(n = 1, stringsAsFactors = FALSE)
        )[["response"]])
      }
      # For other queries, use default mock
      base_mock <- mock_httr_replies(
        mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "SHOW COLUMNS FROM iris",
          data = data.frame(
            Column = c("sepal.length", "sepal.width", "petal.length", "petal.width", "species"),
            stringsAsFactors = FALSE
          )
        ),
        mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "^CREATE TABLE",
          data = data.frame()
        )
      )
      return(base_mock(url, body, ...))
    }
  }
  
  with_mocked_bindings(
    {
      iris_presto <- dplyr::tbl(db, "iris")
      iris_summary <- iris_presto %>%
        dplyr::group_by(species) %>%
        dplyr::summarise(mean_sepal = mean(sepal.length))
      
      result <- dplyr::compute(iris_summary, "test_table", cte = FALSE)
      expect_is(result, "tbl_presto")
    },
    dbListFields = function(conn, name, ...) {
      c("sepal.length", "sepal.width", "petal.length", "petal.width", "species")
    },
    db_sql_render = function(con, sql, ...) {
      dbplyr::sql("SELECT 1")
    },
    httr_POST = create_mock_httr_post(),
    httr_GET = mock_httr_replies()
  )
  
  # Verify dbExistsTable was called 4 times:
  # 1 from db_save_query
  # 3 from our retry logic:
  #   - while condition first check (FALSE, enters loop)
  #   - while condition second check (TRUE, exits loop)
  #   - after-loop check (TRUE)
  expect_equal(dbexists_post_calls, 4)
})

test_that("compute() waits for table existence - multiple retries", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  
  parts <- setup_mock_dplyr_connection()
  db <- parts[["db"]]
  
  dbexists_post_calls <- 0
  
  create_mock_httr_post <- function() {
    function(url, body, ...) {
      # Track dbExistsTable queries
      if (grepl("SELECT COUNT\\(\\*\\) AS n.*FROM information_schema.columns", body)) {
        dbexists_post_calls <<- dbexists_post_calls + 1
        # First call is from db_save_query (should return FALSE)
        if (dbexists_post_calls == 1) {
          return(mock_httr_response(
            url,
            status_code = 200,
            state = "FINISHED",
            request_body = body,
            data = data.frame(n = 0, stringsAsFactors = FALSE)
          )[["response"]])
        }
        # Calls 2-6 are from our retry logic:
        #   - Calls 2-4: while condition checks (3 FALSE)
        #   - Call 5: while condition check (TRUE, exits loop)
        #   - Call 6: after-loop check (TRUE)
        if (dbexists_post_calls <= 4) {
          return(mock_httr_response(
            url,
            status_code = 200,
            state = "FINISHED",
            request_body = body,
            data = data.frame(n = 0, stringsAsFactors = FALSE)
          )[["response"]])
        }
        # Calls 5-6: table exists
        return(mock_httr_response(
          url,
          status_code = 200,
          state = "FINISHED",
          request_body = body,
          data = data.frame(n = 1, stringsAsFactors = FALSE)
        )[["response"]])
        return(mock_httr_response(
          url,
          status_code = 200,
          state = "FINISHED",
          request_body = body,
          data = data.frame(n = 1, stringsAsFactors = FALSE)
        )[["response"]])
      }
      # For other queries, use default mock
      base_mock <- mock_httr_replies(
        mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "SHOW COLUMNS FROM iris",
          data = data.frame(
            Column = c("sepal.length", "sepal.width", "petal.length", "petal.width", "species"),
            stringsAsFactors = FALSE
          )
        ),
        mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "^CREATE TABLE",
          data = data.frame()
        )
      )
      return(base_mock(url, body, ...))
    }
  }
  
  with_mocked_bindings(
    {
      iris_presto <- dplyr::tbl(db, "iris")
      iris_summary <- iris_presto %>%
        dplyr::group_by(species) %>%
        dplyr::summarise(mean_sepal = mean(sepal.length))
      
      result <- dplyr::compute(iris_summary, "test_table", cte = FALSE)
      expect_is(result, "tbl_presto")
    },
    dbListFields = function(conn, name, ...) {
      c("sepal.length", "sepal.width", "petal.length", "petal.width", "species")
    },
    db_sql_render = function(con, sql, ...) {
      dbplyr::sql("SELECT 1")
    },
    httr_POST = create_mock_httr_post(),
    httr_GET = mock_httr_replies()
  )
  
  # Verify dbExistsTable was called 6 times:
  # 1 from db_save_query
  # 5 from our retry logic:
  #   - while condition checks: 4 times (3 FALSE, then TRUE on 4th)
  #   - after-loop check: 1 time (TRUE)
  expect_equal(dbexists_post_calls, 6)
})

test_that("compute() waits for table existence - max retries exceeded", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  
  parts <- setup_mock_dplyr_connection()
  db <- parts[["db"]]
  
  dbexists_post_calls <- 0
  
  create_mock_httr_post <- function() {
    function(url, body, ...) {
      # Track dbExistsTable queries
      if (grepl("SELECT COUNT\\(\\*\\) AS n.*FROM information_schema.columns", body)) {
        dbexists_post_calls <<- dbexists_post_calls + 1
        # Always return table doesn't exist
        return(mock_httr_response(
          url,
          status_code = 200,
          state = "FINISHED",
          request_body = body,
          data = data.frame(n = 0, stringsAsFactors = FALSE)
        )[["response"]])
      }
      # For other queries, use default mock
      base_mock <- mock_httr_replies(
        mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "SHOW COLUMNS FROM iris",
          data = data.frame(
            Column = c("sepal.length", "sepal.width", "petal.length", "petal.width", "species"),
            stringsAsFactors = FALSE
          )
        ),
        mock_httr_response(
          "http://localhost:8000/v1/statement",
          status_code = 200,
          state = "FINISHED",
          request_body = "^CREATE TABLE",
          data = data.frame()
        )
      )
      return(base_mock(url, body, ...))
    }
  }
  
  with_mocked_bindings(
    {
      iris_presto <- dplyr::tbl(db, "iris")
      iris_summary <- iris_presto %>%
        dplyr::group_by(species) %>%
        dplyr::summarise(mean_sepal = mean(sepal.length))
      
      expect_warning(
        result <- dplyr::compute(iris_summary, "test_table", cte = FALSE),
        "not found after 5 attempts"
      )
      expect_is(result, "tbl_presto")  # Still returns despite warning
    },
    dbListFields = function(conn, name, ...) {
      c("sepal.length", "sepal.width", "petal.length", "petal.width", "species")
    },
    db_sql_render = function(con, sql, ...) {
      dbplyr::sql("SELECT 1")
    },
    httr_POST = create_mock_httr_post(),
    httr_GET = mock_httr_replies()
  )
  
  # Verify dbExistsTable was called 7 times:
  # 1 from db_save_query, 6 from our retry logic (initial + 5 retries)
  expect_equal(dbexists_post_calls, 7)
})

test_that("compute() skips retry logic for CTE", {
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    skip("Skipping dplyr tests because we can't load dplyr")
  }
  
  parts <- setup_mock_dplyr_connection()
  db <- parts[["db"]]
  
  dbexists_post_calls <- 0
  
  create_mock_httr_post <- function() {
    base_mock <- mock_httr_replies(
      mock_httr_response(
        "http://localhost:8000/v1/statement",
        status_code = 200,
        state = "FINISHED",
        request_body = "SHOW COLUMNS FROM iris",
        data = data.frame(
          Column = c("sepal.length", "sepal.width", "petal.length", "petal.width", "species"),
          stringsAsFactors = FALSE
        )
      )
    )
    
    function(url, body, ...) {
      # Track dbExistsTable queries
      if (grepl("SELECT COUNT\\(\\*\\) AS n.*FROM information_schema.columns", body)) {
        dbexists_post_calls <<- dbexists_post_calls + 1
      }
      return(base_mock(url, body, ...))
    }
  }
  
  with_mocked_bindings(
    {
      iris_presto <- dplyr::tbl(db, "iris")
      iris_summary <- iris_presto %>%
        dplyr::group_by(species) %>%
        dplyr::summarise(mean_sepal = mean(sepal.length))
      
      result <- dplyr::compute(iris_summary, "test_table", cte = TRUE)
      expect_is(result, "tbl_presto")
    },
    dbListFields = function(conn, name, ...) {
      c("sepal.length", "sepal.width", "petal.length", "petal.width", "species")
    },
    httr_POST = create_mock_httr_post(),
    httr_GET = mock_httr_replies()
  )
  
  # Verify dbExistsTable was never called (CTE doesn't create tables)
  expect_equal(dbexists_post_calls, 0)
})
