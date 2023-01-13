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
    add_chunk(iris, base_chunk_fields = c("Species")),
    iris
  )
})

test_that("add_chunk adds a new field when larger than size limit", {
  chunk_iris <- add_chunk(iris, chunk_size = 2000)
  expect_equal(
    colnames(chunk_iris),
    c(colnames(iris), "aux_chunk_idx")
  )
  expect_equal(class(chunk_iris$aux_chunk_idx), "integer")
  chunk_iris_2 <-
    add_chunk(iris, chunk_size = 2000, new_chunk_field_name = "chunk")
  expect_equal(
    colnames(chunk_iris_2),
    c(colnames(iris), "chunk")
  )
  expect_equal(class(chunk_iris_2$chunk), "integer")
  chunk_iris_field <-
    add_chunk(iris, chunk_size = 2000, base_chunk_fields = c("Species"))
  expect_equal(
    colnames(chunk_iris_field),
    c(colnames(iris), "aux_chunk_idx")
  )
  expect_equal(class(chunk_iris_field$aux_chunk_idx), "integer")
  expect_equal(
    nrow(dplyr::count(chunk_iris_field, Species, aux_chunk_idx)),
    9L
  )
})
