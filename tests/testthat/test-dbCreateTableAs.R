# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('dbCreateTableAs and sqlCreateTableAs')

source('utilities.R')

test_that('sqlCreateTableAs works', {
  conn <- setup_live_connection()
  test_table_name <- 'test_sqlcreatetableas'
  expect_equal(
    sqlCreateTableAs(
      conn, test_table_name,
      statement = 'SELECT * FROM iris'
    ),
    DBI::SQL(
      paste0(
        'CREATE TABLE ', dbQuoteIdentifier(conn, test_table_name), ' AS\n',
        'SELECT * FROM iris\n'
      )
    )
  )
  expect_equal(
    sqlCreateTableAs(
      conn, test_table_name,
      statement = 'SELECT * FROM iris',
      with = 'WITH (format = \'ORC\')'
    ),
    DBI::SQL(
      paste0(
        'CREATE TABLE ', dbQuoteIdentifier(conn, test_table_name), ' AS\n',
        'SELECT * FROM iris\n',
        'WITH (format = \'ORC\')'
      )
    )
  )
})

test_that('dbCreateTableAS works with live database', {
  conn <- setup_live_connection()
  test_table_name <- 'test_createtableas'
  if (dbExistsTable(conn, test_table_name)) {
    dbRemoveTable(conn, test_table_name)
  }
  expect_false(dbExistsTable(conn, test_table_name))
  expect_true(dbCreateTableAs(conn, test_table_name, 'SELECT * FROM iris'))
  expect_true(dbExistsTable(conn, test_table_name))
  expect_equal(
    dbListFields(conn, test_table_name),
    dbListFields(conn, 'iris')
  )
  expect_equal(
    get_nrow(conn, test_table_name),
    get_nrow(conn, 'iris')
  )
  expect_error(
    dbCreateTableAs(conn, test_table_name, 'SELECT * FROM iris'),
    'Destination table .* already exists'
  )
})
