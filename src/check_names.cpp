// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved

// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree.
#include <Rcpp.h>
using namespace Rcpp;

// jsonify a character vector
std::string jsonify(CharacterVector x) {
  Function paste = Environment("package:base")["paste"];
  std::string joined = as<std::string>(paste(x, Named("collapse", ", ")));
  return "[" + joined + "]";
}

// x is a list of list. verify all sublists have same number of element as
// column count. verify all sublists have same names if available.
// [[Rcpp::export]]
SEXP check_names(List x, int column_count) {
  CharacterVector column_names;

  for (int i = 0; i < x.size(); i++) {
    List row = x[i];

    if (row.size() != column_count) {
      stop("Item " + toString(i) + ", " +
           "expected: " + toString(column_count) + " columns, " +
           "received: " + toString(row.size()));
    }

    if (!Rf_isNull(row.names())) {
      CharacterVector names = row.names();
      // item is a named list
      if (column_names.size() > 0) {
        // we have 'seen' column names in previous items
        for (int j = 0; j < column_names.size(); j++) {
          if (column_names[j] != names[j]) {
            // We have a different column name set from what we have seen before
            warning("Item " + toString(i) + ", column names differ across rows, " +
              "expected: " + jsonify(column_names) + ", " +
              "received: " + jsonify(names));
            break;
          }
        }
      } else {
        // First time we see a named item, use the names for the item as
        // column names for the resulting data.frame
        column_names = names;
      }
    }
  }

  if (column_names.size() == 0) {
    return R_NilValue;
  }
  return column_names;
}


// You can include R code blocks in C++ files processed with sourceCpp
// (useful for testing and development). The R code will be automatically
// run after the compilation.
//

/*** R
check_names(list(), 0)
check_names(list(list(1, 2)), 2)
check_names(list(list(a = 1, b = 2), list(a = 2, b = 3)), 2)
check_names(list(list(a = 1, b = 2), list(a = 2, c = 3)), 2)
check_names(list(list(a = 1, b = 2), list(a = 2)), 2)
try({check_names(list(list(a = 1), list(a = 2)), 2)})
*/
