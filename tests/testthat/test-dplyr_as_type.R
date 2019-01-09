# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context('dplyr_as.<type>')

source('utilities.R')

with_locale(test.locale(), test_that)('dplyr as.<type> works with live database', {
  parts <- setup_live_dplyr_connection()
  db <- parts[['db']]

  t <- dplyr::tbl(db, dplyr::sql("SELECT 1 AS a"))

  result <- dplyr::transmute(
    t,
    null_to_character=as.character(NA),
    integer_to_character=as.character(1L),
    boolean_to_character=as.character(FALSE),
    null_to_integer=as.integer(NA),
    character_to_integer=as.integer("123"),
    boolean_to_integer=as.integer(TRUE),
    numeric_to_integer=as.integer(3.0),
    null_to_double=as.numeric(NA),
    integer_to_double=as.numeric(5L),
    integer_to_double_v2=as.double(6L),
    character_to_double=as.numeric("1.1"),
    boolean_to_double=as.numeric(TRUE),
    null_to_date=as.Date(NA),
    character_to_date=as.Date('2018-03-01'),
    null_to_boolean=as.logical(NA),
    integer_to_boolean=as.logical(0L),
    character_to_boolean=as.logical("true"),
    double_to_boolean=as.logical(-1.0),
    null_to_varbinary=as.raw(NA),
    character_to_varbinary=as.raw('a'),
    nested=as.integer(as.logical("true"))
  )

  expected <- data.frame(
    null_to_character=NA_character_,
    integer_to_character="1",
    boolean_to_character="false",
    null_to_integer=NA_integer_,
    character_to_integer=123L,
    boolean_to_integer=1L,
    numeric_to_integer=3L,
    null_to_double=NA_real_,
    integer_to_double=5.0,
    integer_to_double_v2=6.0,
    character_to_double=1.1,
    boolean_to_double=1.0,
    null_to_date=structure(NA_real_, class='Date'),
    character_to_date=as.Date('2018-03-01'),
    null_to_boolean=NA,
    integer_to_boolean=FALSE,
    character_to_boolean=TRUE,
    double_to_boolean=TRUE,
    null_to_varbinary=NA,
    character_to_varbinary=NA,
    nested=1L,
    stringsAsFactors=FALSE
  )
  expected[['null_to_varbinary']] <- list(NA)
  expected[['character_to_varbinary']] <- list(charToRaw('a'))

  expect_equal_data_frame(
    as.data.frame(dplyr::collect(result)),
    expected
  )
})
