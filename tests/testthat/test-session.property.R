# Copyright (c) 2015-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree. An additional grant
# of patent rights can be found in the PATENTS file in the same directory.

context("session.property")

source('utilities.R')

query_session_property <- function(conn, property) {
  properties <- dbGetQuery(conn, 'SHOW SESSION')
  properties[properties[['Name']] == property, 'Value', drop=TRUE]
}

invert_boolean_property <- function(property) {
  if (property == 'true') 'false' else 'true'
}

test_that('session properties can be configured in connection', {
  conn <- setup_live_connection()
  expect_equal(conn@session$parameters(), list())

  # verify default value is applied
  optimize_hash_generation <- 'optimize_hash_generation'
  optimize_hash_generation_default <- query_session_property(conn, optimize_hash_generation)
  optimize_hash_generation_inverse <- invert_boolean_property(optimize_hash_generation_default)

  # property can be set via connection parameters
  conn <- setup_live_connection(
    parameters = setNames(list(optimize_hash_generation_inverse), optimize_hash_generation)
  )
  expect_equal(query_session_property(conn, optimize_hash_generation), optimize_hash_generation_inverse)

  # reset parameters
  conn <- setup_live_connection()

  # property can be set via query
  dbGetQuery(conn, paste0('SET SESSION ', optimize_hash_generation, '=', optimize_hash_generation_inverse))
  # property is updated in connection
  expect_equal(conn@session$parameters(), setNames(list(optimize_hash_generation_inverse), optimize_hash_generation))
  # property is reflected in subsequent queries
  expect_equal(query_session_property(conn, optimize_hash_generation), optimize_hash_generation_inverse)

  # additional property can be set
  columnar_processing <- 'columnar_processing'
  columnar_processing_default <- query_session_property(conn, columnar_processing)
  columnar_processing_inverse <- invert_boolean_property(columnar_processing_default)
  dbGetQuery(conn, paste0('SET SESSION ', columnar_processing, '=', columnar_processing_inverse))
  # proerty is set
  expect_equal(query_session_property(conn, columnar_processing), columnar_processing_inverse)
  # previous property is preserved
  expect_equal(query_session_property(conn, optimize_hash_generation), optimize_hash_generation_inverse)

  # property can be reset via query
  dbGetQuery(conn, paste('RESET SESSION', optimize_hash_generation))
  # property is cleared in connection and the remaining is preserved
  expect_equal(conn@session$parameters(), setNames(list(columnar_processing_inverse), columnar_processing))
  # property has been reset in subsequent queries
  expect_equal(query_session_property(conn, optimize_hash_generation), optimize_hash_generation_default)
})
