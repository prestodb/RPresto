# Copyright (c) Facebook, Inc. and its affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

context('response.to.content')

source('utilities.R')

with_locale(test.locale(), test_that)('response.to.content works', {
  response.to.content <- RPresto:::response.to.content

  response <- mock_httr_response(
      'dummy_url',
      state='dummy_state',
      status_code=0,
      extra_content=list('extra'=jsonlite::unbox('content')),
      data=data.frame(
        l=TRUE,
        m=NA_integer_,
        n={ x <- 'öğışüçÖĞİŞÜÇ'; Encoding(x) <- 'UTF-8'; x },
        stringsAsFactors=FALSE
      )
  )[['response']]
  expect_equal(
    response.to.content(response),
    list(
      extra='content',
      stats=list(state='dummy_state'),
      id='dummy_url',
      columns=list(
        list(
          name='l',
          type='boolean',
          typeSignature=list(
            rawType='boolean',
            typeArguments=list(),
            literalArguments=list()
          )
        ),
        list(
          name='m',
          type='bigint',
          typeSignature=list(
            rawType='bigint',
            typeArguments=list(),
            literalArguments=list()
          )
        ),
        list(
          name='n',
          type='varchar',
          typeSignature=list(
            rawType='varchar',
            typeArguments=list(),
            literalArguments=list()
          )
        )
      ),
      data=list(
        list(
          TRUE,
          "NA",
          { x <- 'öğışüçÖĞİŞÜÇ'; Encoding(x) <- 'UTF-8'; x }
        )
      )
    )
  )
})
