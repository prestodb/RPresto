# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context("add_chunk")

source("utilities.R")

test_that("add_chunk returns the original data frame if within size", {
  expect_equal_data_frame(
    add_chunk(iris),
    iris
  )
  expect_equal_data_frame(
    add_chunk(iris, chunk_fields = c("Species")),
    iris
  )
})

test_that("add_chunk adds a new field when larger than size limit", {
  expect_equal(
    colnames(add_chunk(iris, chunk_size = 2000)),
    c(colnames(iris), "chunk_idx")
  )
  expect_equal(
    colnames(add_chunk(iris, chunk_size = 2000, new_chunk_field_name = "chunk")),
    c(colnames(iris), "chunk")
  )
  new_iris <- add_chunk(iris, chunk_size = 2000, chunk_fields = c("Species"))
  expect_equal(
    colnames(new_iris),
    c(colnames(iris), "chunk_idx")
  )
  expect_equal(
    nrow(dplyr::count(new_iris, Species, chunk_idx)),
    6L
  )
})
