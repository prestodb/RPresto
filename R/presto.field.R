# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

parse.presto.type <- function(presto_type) {
  if (is.na(presto_type)) {
    return(presto_type)
  } else if (presto_type == "boolean") {
    return("PRESTO_BOOLEAN")
  } else if (presto_type %in% c("tinyint", "smallint", "integer")) {
    return("PRESTO_INTEGER")
  } else if (presto_type %in% c("bigint")) {
    return("PRESTO_BIGINT")
  } else if (presto_type %in% c("real", "double")) {
    return("PRESTO_FLOAT")
  } else if (presto_type %in% c("decimal", "json", "varchar", "char")) {
    return("PRESTO_STRING")
  } else if (presto_type == "varbinary") {
    return("PRESTO_BYTES")
  } else if (presto_type == "date") {
    return("PRESTO_DATE")
  } else if (presto_type == "timestamp") {
    return("PRESTO_TIMESTAMP")
  } else if (presto_type == "timestamp with time zone") {
    return("PRESTO_TIMESTAMP_WITH_TZ")
  } else if (presto_type == "time") {
    return("PRESTO_TIME")
  } else if (presto_type == "time with time zone") {
    return("PRESTO_TIME_WITH_TZ")
  } else if (presto_type == "interval year to month") {
    return("PRESTO_INTERVAL_YEAR_TO_MONTH")
  } else if (presto_type == "interval day to second") {
    return("PRESTO_INTERVAL_DAY_TO_SECOND")
  } else if (presto_type == "map") {
    return("PRESTO_MAP")
  } else if (presto_type == "row") {
    return("PRESTO_ROW")
  } else if (presto_type == "unknown") {
    # This happens in an empty array. So we use integer type as a default.
    return("PRESTO_INTEGER")
  } else {
    warning(
      "Type [", presto_type, "] cannot be parsed. The field will be coerced ",
      "to a character field in R.",
      call. = FALSE
    )
    return("PRESTO_UNKNOWN")
  }
}

create.presto.field <- function(name, type, key_type = NA_character_,
                                is_array = NA, fields = list(),
                                is_parent_map = FALSE, is_parent_array = FALSE,
                                session_timezone = NA_character_,
                                output_timezone = NA_character_,
                                timestamp = as.POSIXct(NULL)) {
  prf <- list()
  prf$name_ <- name
  prf$type_ <- parse.presto.type(type)
  prf$key_type_ <- parse.presto.type(key_type)
  prf$is_array_ <- is_array
  prf$is_map_ <- (type == "map")
  prf$is_row_ <- (type == "row")
  prf$fields_ <- fields
  prf$session_timezone_ <- session_timezone
  prf$output_timezone_ <- output_timezone
  prf$timestamp_ <- timestamp
  prf$is_parent_map_ <- is_parent_map
  prf$is_parent_array_ <- is_parent_array
  class(prf) <- "presto.field"
  return(prf)
}

init.presto.field.from.json <- function(column.list, session.timezone, output.timezone, timestamp, is_parent_map = FALSE, is_parent_array = FALSE) {
  stopifnot(is.list(column.list))

  name <- purrr::pluck(column.list, "name")
  type <- purrr::pluck(column.list, "typeSignature", "rawType")
  is_array <- (type == "array")
  if (is_array) {
    type_signature <-
      purrr::pluck(column.list, "typeSignature", "typeArguments", 1) %||%
      purrr::pluck(column.list, "typeSignature", "arguments", 1, "value")
    type <- purrr::pluck(type_signature, "rawType")
    if (type == "array") {
      stop(
        "The field [", name, "] is an ARRAY of ARRAY which is not supported",
        call. = FALSE
      )
    }
  } else {
    type_signature <- purrr::pluck(column.list, "typeSignature")
  }
  # Filled only for MAP
  key_type <- NA_character_
  # Filled for complex types (i.e., MAP and ROW)
  fields <- list()
  if (type == "map") {
    key_type <-
      purrr::pluck(type_signature, "typeArguments", 1, "rawType") %||%
      purrr::pluck(type_signature, "arguments", 1, "value", "rawType")
    fields_json <- list(
      list(
        name = name,
        type = "map_field_type",
        typeSignature =
          purrr::pluck(type_signature, "typeArguments", 2) %||%
          purrr::pluck(type_signature, "arguments", 2, "value")
      )
    )
    fields <- purrr::map(
      fields_json, init.presto.field.from.json, session.timezone, output.timezone, timestamp,
      is_parent_map = TRUE, is_parent_array = is_array
    )
  }
  # ROW type is a complex type that has sub-fields
  if (type == "row") {
    fields_count <- length(
      purrr::pluck(type_signature, "literalArguments") %||%
      purrr::pluck(type_signature, "arguments")
    )
    fields_json <- vector(mode = "list", length = fields_count)
    for (i in seq(fields_count)) {
      fields_json[[i]]$name <-
        purrr::pluck(type_signature, "literalArguments", i) %||%
        purrr::pluck(type_signature, "arguments", i, "value", "fieldName", "name")
      field_type <- purrr::pluck(type_signature, "arguments", i, "value", "typeSignature")
      if (!is.character(field_type)) {
        field_type <- purrr::pluck(type_signature, "arguments", i, "value", "typeSignature", "rawType")
      }
      fields_json[[i]]$type <- field_type
      fields_json[[i]]$typeSignature <-
        purrr::pluck(type_signature, "typeArguments", i) %||%
        purrr::pluck(type_signature, "arguments", i, "value", "typeSignature")
    }
    fields <- purrr::map(
      fields_json, init.presto.field.from.json, session.timezone, output.timezone, timestamp,
    )
  }

  if (
    type %in% c(
      "timestamp",
      "timestamp with time zone",
      "time with time zone"
    )
  ) {
    prf <- create.presto.field(
      name, type, key_type, is_array, fields, is_parent_map, is_parent_array,
      session.timezone, output.timezone, timestamp
    )
  } else {
    prf <- create.presto.field(
      name, type, key_type, is_array, fields, is_parent_map, is_parent_array
    )
  }
  return(prf)
}

get.process.func <- function(prf) {
  stopifnot(is_simple_type(prf))
  process.func <- if (prf$type_ == "PRESTO_STRING") {
    purrr::flatten_chr
  } else if (prf$type_ == "PRESTO_INTEGER") {
    purrr::flatten_int
  } else if (prf$type_ == "PRESTO_BIGINT") {
    function(x) unlist_with_attr(purrr::map(x, bit64::as.integer64))
  } else if (prf$type_ == "PRESTO_BOOLEAN") {
    purrr::flatten_lgl
  } else if (prf$type_ == "PRESTO_FLOAT") {
    function(x) purrr::flatten_dbl(purrr::map(x, ~ as.numeric(.)))
  } else if (prf$type_ == "PRESTO_BYTES") {
    function(x) purrr::map(x, ~ openssl::base64_decode(as.character(.)))
  } else if (prf$type_ == "PRESTO_DATE") {
    function(x) as.Date(purrr::flatten_chr(x))
  } else if (prf$type_ == "PRESTO_TIMESTAMP") {
    function(x) {
      lubridate::with_tz(
        lubridate::ymd_hms(purrr::flatten_chr(x), tz = prf$session_timezone_),
        tz = prf$output_timezone_
      )
    }
  } else if (prf$type_ == "PRESTO_TIMESTAMP_WITH_TZ") {
    function(x) {
      unlist_with_attr(
        purrr::map(x, parse.timestamp_with_tz, output_timezone = prf$output_timezone_)
      )
    }
  } else if (prf$type_ == "PRESTO_TIME") {
    function(x) {
      if (!requireNamespace("hms", quietly = TRUE)) {
        stop("The [", x$name_, "] field is a TIME field. Please ",
          "install the hms package or cast the field to VARCHAR before ",
          "importing it to R.",
          call. = FALSE
        )
      }
      unlist_with_attr(purrr::map(x, hms::as_hms))
    }
  } else if (prf$type_ == "PRESTO_TIME_WITH_TZ") {
    function(x) {
      unlist_with_attr(
        purrr::map(
          x,
          parse.time_with_tz,
          output_timezone = prf$output_timezone_, timestamp = prf$timestamp_
        )
      )
    }
  } else if (prf$type_ == "PRESTO_INTERVAL_YEAR_TO_MONTH") {
    function(x) {
      unlist_with_attr(purrr::map(x, parse.interval_year_to_month))
    }
  } else if (prf$type_ == "PRESTO_INTERVAL_DAY_TO_SECOND") {
    function(x) {
      unlist_with_attr(purrr::map(x, parse.interval_day_to_second))
    }
  } else if (prf$type_ == "PRESTO_UNKNOWN") {
    purrr::flatten_chr
  } else {
    stop(
      "Cannot find processing function for the [", prf$name_,
      "] field which is ", prf$type_,
      call. = FALSE
    )
  }

  return(
    function(x, keep_names = TRUE, ...) {
      if (!is.list(x)) x <- list(x)
      nms <- names(x)
      x <- replace_null_with_na(x)
      x <- process.func(x)
      if (keep_names) {
        return(purrr::set_names(x, nms))
      } else {
        return(x)
      }
    }
  )
}

create.empty.tibble <- function(schema) {
  stopifnot(is.list(schema))
  rv <- list()
  for (prf in schema) {
    # primitive types
    if (!prf$is_array_ & !prf$is_map_ & !prf$is_row_) {
      rv[[prf$name_]] <-
        if (prf$type_ == "PRESTO_INTEGER") {
          integer(0)
        } else if (prf$type_ == "PRESTO_BIGINT") {
          bit64::integer64(0)
        } else if (prf$type_ == "PRESTO_BOOLEAN") {
          logical(0)
        } else if (prf$type_ == "PRESTO_STRING") {
          character(0)
        } else if (prf$type_ == "PRESTO_FLOAT") {
          numeric(0)
        } else if (prf$type_ == "PRESTO_BYTES") {
          list()
        } else if (prf$type_ == "PRESTO_DATE") {
          as.Date(character(0))
        } else if (
          prf$type_ %in% c("PRESTO_TIMESTAMP", "PRESTO_TIMESTAMP_WITH_TZ")
        ) {
          as.POSIXct(character(0))
        } else if (prf$type_ %in% c("PRESTO_TIME", "PRESTO_TIME_WITH_TZ")) {
          if (!requireNamespace("hms", quietly = TRUE)) {
            stop("The [", prf$name_, "] field is a TIME field. Please ",
              "install the hms package or cast the field to VARCHAR before ",
              "importing it to R.",
              call. = FALSE
            )
          }
          hms::as_hms(character(0))
        } else if (
          prf$type_ %in% c(
            "PRESTO_INTERVAL_YEAR_TO_MONTH",
            "PRESTO_INTERVAL_DAY_TO_SECOND"
          )
        ) {
          lubridate::duration(integer(0))
        } else if (prf$type_ == "PRESTO_UNKNOWN") {
          character(0)
        }
    } else {
      # For map and row, it's always a list
      rv[[prf$name_]] <- list()
    }
  }
  return(tibble::as_tibble(rv))
}

# data contains all rows of data in a list
organize_simple_type_data <- function(data, prf, keep_names) {
  # primitive (simple) types -> scalars
  if (!prf$is_array_) {
    data <- get.process.func(prf)(data, keep_names)
  }
  # array of primitive (simple) types -> vectors
  if (prf$is_array_) {
    data <- purrr::map(data, get.process.func(prf))
  }
  return(data)
}

# data contains all rows of data in a list
organize_map_type_data <- function(data, prf, keep_names) {
  .organize_map_type_data <- function(data, prf, keep_names) {
    if (is_simple_type(prf)) {
      data <- purrr::map(data, organize_simple_type_data, prf, keep_names)
      return(data)
    }
    if (prf$is_row_) {
      data <- organize_row_type_data(data, prf, keep_names)
      return(data)
    }
    # prf is another map
    data <- purrr::map(
      data, organize_map_type_data, prf$fields_[[1]], keep_names
    )
    return(data)
  }
  if (!prf$is_parent_array_) {
    return(.organize_map_type_data(data, prf, keep_names))
  } else {
    return(purrr::map(data, .organize_map_type_data, prf, keep_names))
  }
}

# data contains all rows of data in a list
organize_row_type_data <- function(data, prf, keep_names) {
  field.count <- length(prf$fields_)
  field.names <- purrr::map_chr(prf$fields_, ~ .$name_)
  if (!prf$is_array_) {
    # single row -> named list
    if (!prf$is_parent_map_) {
      data <- purrr::map(data, ~ organize.data.by.schema(list(.), prf$fields_))
    }
    # map of 1 row -> named list
    # map of n rows -> tibble
    if (prf$is_parent_map_) {
      element.count <- purrr::map_int(data, length)
      if (!all(element.count == mean(element.count))) {
        stop(
          "Element counts in the [", prf$name_,
          "] field are not consistent across rows.",
          call. = FALSE
        )
      }
      element.count <- mean(element.count)
      if (element.count > 1) {
        # tibble
        data <- purrr::map(
          data,
          ~ organize.data.by.schema(., prf$fields_, keep_names = FALSE)
        )
        data <- purrr::map(data, ~ tibble::as_tibble(.))
      } else {
        data <- purrr::map(
          data,
          # MAP keys (akin to row names) are discarded here
          ~ organize.data.by.schema(., prf$fields_, keep_names = FALSE)
        )
      }
    }
  }
  # array of rows -> tibble
  if (prf$is_array_) {
    data <- purrr::map(data, ~ organize.data.by.schema(., prf$fields_))
    data <- purrr::map(data, ~ tibble::as_tibble(.))
  }
  return(data)
}

organize.data.by.schema <- function(data, schema, keep_names = TRUE) {
  if (is.null(schema) || (length(schema) == 0)) {
    return(tibble::tibble())
  } else {
    col.count <- length(schema)
    if (length(data) > 0) {
      n_columns_by_row <- purrr::map_int(data, length)
      if (!all(n_columns_by_row == mean(n_columns_by_row))) {
        stop(
          "Column counts in data are not the same across rows.",
          call. = FALSE
        )
      }
      if (col.count != mean(n_columns_by_row)) {
        stop(
          "Column counts from schema and data are not the same.",
          call. = FALSE
        )
      }
    }
    col.names <- purrr::map_chr(schema, ~ .$name_)
  }
  # Special case used in db_query_fields
  if (is.null(data) || length(data) == 0) {
    empty_tibble <- create.empty.tibble(schema)
    return(empty_tibble)
  }

  data <- purrr::transpose(data)
  names(data) <- col.names
  for (col.idx in seq(col.count)) {
    prf <- schema[[col.idx]]
    # primitive (simple) types
    if (is_simple_type(prf)) {
      data[[col.idx]] <- organize_simple_type_data(
        data[[col.idx]], prf, keep_names
      )
    }
    # map type
    if (prf$is_map_) {
      # process all rows
      data[[col.idx]] <- organize_map_type_data(
        data[[col.idx]], prf$fields_[[1]], keep_names
      )
    }
    # row type
    if (prf$is_row_) {
      data[[col.idx]] <- organize_row_type_data(
        data[[col.idx]], prf, keep_names
      )
    }
  }
  return(data)
}

extract.data <-
  function(response.content,
           session.timezone,
           output.timezone,
           timestamp,
           bigint = c("integer", "integer64", "numeric", "character")) {
    bigint <- match.arg(bigint)
    columns <- response.content$columns
    schema <- purrr::map(
      columns, init.presto.field.from.json,
      session.timezone = session.timezone,
      output.timezone = output.timezone,
      timestamp = timestamp
    )
    data <- response.content$data
    data <- organize.data.by.schema(data, schema)
    data <- tibble::as_tibble(data)
    data <- convert_bigint(data, bigint)
    return(data)
  }
