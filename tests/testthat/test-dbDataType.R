# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('dbDataType')

with_locale(test.locale(), test_that)('presto simple types are correct', {

    drv <- RPresto::Presto()

    expect_equal(dbDataType(drv, NULL), 'VARCHAR')
    expect_equal(dbDataType(drv, TRUE), 'BOOLEAN')
    expect_equal(dbDataType(drv, 1L), 'BIGINT')
    expect_equal(dbDataType(drv, 1.0), 'DOUBLE')
    expect_equal(dbDataType(drv, ''), 'VARCHAR')
    expect_equal(dbDataType(drv, vector('raw', 0)), 'VARBINARY')
    expect_equal(dbDataType(drv, as.Date('2015-03-01')), 'DATE')
    expect_equal(
      dbDataType(drv, as.POSIXct('2015-03-01 12:00:00')),
      'TIMESTAMP'
    )
    expect_equal(
      dbDataType(drv, as.POSIXct('2015-03-01 12:00:00', tz='UTC')),
      'TIMESTAMP WITH TIME ZONE'
    )
    expect_equal(dbDataType(drv, factor()), 'VARCHAR')
    expect_equal(dbDataType(drv, factor(ordered=TRUE)), 'VARCHAR')
    expect_equal(
      dbDataType(drv, structure(list(), class='test_class')),
      'VARCHAR'
    )
})

test_that('conversion to array is correct', {
    drv <- RPresto::Presto()

    expect_equal(dbDataType(drv, list()), 'ARRAY<VARCHAR>')
    expect_equal(dbDataType(drv, list(list())), 'ARRAY<ARRAY<VARCHAR>>')
    expect_equal(dbDataType(drv,
      list(as.Date('2015-03-01'), as.Date('2015-03-02'))),
      'ARRAY<DATE>'
    )
    expect_equal(dbDataType(drv,
      list(list(as.Date('2015-03-01')))),
      'ARRAY<ARRAY<DATE>>'
    )
    expect_equal(dbDataType(drv,
      list(1L, list(a=2, b=3))),
      'VARCHAR'
    )
    expect_equal(dbDataType(drv,
      list(1, 2, 3L)),
      'VARCHAR'
    )
})

test_that('conversion to map is correct', {
    drv <- RPresto::Presto()

    l <- structure(list(), names=character(0))
    expect_equal(dbDataType(drv, l), 'MAP<VARCHAR, VARCHAR>')
    expect_equal(
      dbDataType(drv, list(a=l)),
      'MAP<VARCHAR, MAP<VARCHAR, VARCHAR>>'
    )
    expect_equal(dbDataType(drv,
      list(a=as.Date('2015-03-01'), b=as.Date('2015-03-02'))),
      'MAP<VARCHAR, DATE>'
    )
    expect_equal(dbDataType(drv,
      list(a=list(b=as.Date('2015-03-01')))),
      'MAP<VARCHAR, MAP<VARCHAR, DATE>>'
    )
    expect_equal(dbDataType(drv,
      list(a=1L, b=list(a=2, b=3))),
      'VARCHAR'
    )
})
