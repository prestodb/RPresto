# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

expect_equal_data_frame <- function(r, e, ...) {
  expect_equal(r, e, ...)
  data.classes <- unlist(lapply(e, data.class))
  for (column in which(data.classes %in% "POSIXct")) {
    # all.equal.POSIXt does not check for timezone equality, even when
    # check.attributes=TRUE
    expect_equal(
      attr(r[[column]], "tzone"),
      attr(e[[column]], "tzone")
    )
  }
}

test.timezone <- function() {
  return("Asia/Kathmandu")
}

test.output.timezone <- function() {
  return("America/New_York")
}

test.locale <- function() {
  if (.Platform[["OS.type"]] == "windows") {
    return("Turkish_Turkey.1254")
  }
  return("tr_TR.iso8859-9")
}

with_locale <- function(locale, f) {
  wrapped <- function(desc, ...) {
    old.locale <- Sys.getlocale("LC_CTYPE")
    tryCatch(
      {
        Sys.setlocale("LC_CTYPE", locale)
      },
      warning = function(w) {
        warning.message <- conditionMessage(w)
        if (!grepl(
          "OS reports request to set locale to .* cannot be honored",
          warning.message
        )) {
          warning(w)
        }
      }
    )

    on.exit(Sys.setlocale("LC_CTYPE", old.locale), add = TRUE)
    new.locale <- Sys.getlocale("LC_CTYPE")
    if (new.locale != locale) {
      return(test_that(desc = desc, {
        skip(paste0(
          "Cannot set locale to ", locale,
          "it is set at: ", new.locale
        ))
      }))
    }
    return(f(desc = desc, ...))
  }
  return(wrapped)
}

get_nrow <- function(con, tbl) {
  df <- dbGetQuery(con, paste("SELECT COUNT(*) AS n FROM", tbl))
  return(df$n)
}

tz_to_offset_sec <- function(timezone, dt = Sys.Date()) {
  offset <- as.integer(lubridate::force_tz(as.POSIXlt(dt), tz = timezone)$gmtoff)
  if (is.null(offset)) offset <- 0L
  return(offset)
}

tz_to_offset_hr <- function(timezone, dt = Sys.Date()) {
  offset_sec <- tz_to_offset_sec(timezone, dt)
  as.integer(floor(abs(offset_sec) / 3600))
}

tz_to_offset_min <- function(timezone, dt = Sys.Date()) {
  offset_sec <- tz_to_offset_sec(timezone, dt)
  as.integer((abs(offset_sec) %% 3600) / 60)
}

tz_to_offset <- function(timezone, dt = Sys.Date()) {
  offset_sec <- tz_to_offset_sec(timezone, dt)
  offset_sign <- ifelse(offset_sec >= 0, "+", "-")
  offset_hour <- tz_to_offset_hr(timezone, dt)
  offset_hour_string <- stringi::stri_pad_left(offset_hour, 2, "0")
  offset_min <- tz_to_offset_min(timezone, dt)
  offset_min_string <- stringi::stri_pad_left(offset_min, 2, "0")
  paste0(offset_sign, offset_hour_string, ":", offset_min_string)
}

data.to.list <- function(data) {
  # Change POSIXct representation, otherwise microseconds
  # are chopped off in toJSON
  old.digits.secs <- options("digits.secs" = 3)
  on.exit(options(old.digits.secs), add = TRUE)

  presto.types <- lapply(data, function(l) {
    if (is.list(l)) {
      rs.class <- data.class(l[[1]])
      if (rs.class == "raw") {
        rv <- "varbinary"
      } else if (rs.class == "list") {
        rv <- vector(mode = "character")
        if (is.null(names(l[[1]]))) {
          rv[1] <- "array"
          if (length(l[[1]]) > 0) {
            rv[2] <- dbDataType(RPresto::Presto(), l[[1]][[1]])
          } else {
            rv[2] <- "double"
          }
        } else {
          rv[1] <- "map"
          if (length(l[[1]]) > 0) {
            rv[2] <- dbDataType(RPresto::Presto(), names(l[[1]])[1])
            rv[3] <- dbDataType(RPresto::Presto(), l[[1]][[1]])
          } else {
            rv[2] <- "varchar"
            rv[3] <- "double"
          }
        }
      } else {
        stop("Unsupported mock data type: ", rs.class)
      }
    } else {
      rv <- dbDataType(RPresto::Presto(), l)
    }
    return(rv)
  })

  column.data <- list()
  for (i in seq_along(presto.types)) {
    presto.type <- stringi::stri_trans_tolower(
      presto.types[[i]],
      "en_US.UTF-8"
    )
    column.data[[i]] <- list(
      name = jsonlite::unbox(colnames(data)[i]),
      type = jsonlite::unbox(presto.type[1]),
      typeSignature = list(
        rawType = jsonlite::unbox(presto.type[1]),
        typeArguments = if (length(presto.type) == 1) {
          list()
        } else {
          if (presto.type[1] == "array") {
            list(
              list(rawType = jsonlite::unbox(presto.type[2]))
            )
          } else {
            list(
              list(rawType = jsonlite::unbox(presto.type[2])),
              list(rawType = jsonlite::unbox(presto.type[3]))
            )
          }
        },
        literalArguments = list()
      )
    )
    if (presto.type[1] %in% c("array", "map")) {
      # Lists need to be unboxed
      data[[i]] <- lapply( # Apply to each row
        data[[i]],
        function(l) lapply(l, jsonlite::unbox) # Apply to each item
      )
    }
  }
  return(list(column.data = column.data, data = data))
}

data_frame <- function(...) {
  data.frame(..., stringsAsFactors = FALSE)
}
