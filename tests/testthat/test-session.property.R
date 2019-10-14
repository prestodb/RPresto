# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

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
  potential_properties <- c(
    'columnar_processing',
    'reorder_joins',
    'optimize_metadata_queries',
    'colocated_join',
    'distributed_join'
  )
  for (additional_property in potential_properties) {
    additional_default <- query_session_property(conn, additional_property)
    if (length(additional_default)) {
      break
    }
  }
  if (length(additional_default) == 0) {
    skip('Cannot find additional property with a default value')
  }

  additional_inverse <- invert_boolean_property(additional_default)
  dbGetQuery(conn, paste0('SET SESSION ', additional_property, '=', additional_inverse))
  # property is set
  expect_equal(query_session_property(conn, additional_property), additional_inverse)
  # previous property is preserved
  expect_equal(query_session_property(conn, optimize_hash_generation), optimize_hash_generation_inverse)

  # property can be reset via query
  dbGetQuery(conn, paste('RESET SESSION', optimize_hash_generation))
  # property is cleared in connection and the remaining is preserved
  expect_equal(conn@session$parameters(), setNames(list(additional_inverse), additional_property))
  # property has been reset in subsequent queries
  expect_equal(query_session_property(conn, optimize_hash_generation), optimize_hash_generation_default)
})
