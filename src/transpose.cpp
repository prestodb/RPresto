// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved
#include <Rcpp.h>
using namespace Rcpp;

// copy `c`th column from a row major list `input` to `output` column
template <typename T, class CPPTYPE>
inline void copy_column(T column, int row_count, List input, int c) {
  for (int r = 0; r < row_count; r++) {
    List row = input[r];
    RObject cell = row[c];
    if (!cell.isNULL()) {
      column[r] = as<CPPTYPE>(cell);
    }
  }
}

// set null element in list to NA (NA_LOGICAL) in-place recursively
void null_to_na(List x) {
  int size = x.size();
  for (int i = 0; i < size; i++) {
    RObject element = x[i];
    if (element.isNULL()) {
      x[i] = LogicalVector::create(NA_LOGICAL);
    } else if(element.sexp_type() == VECSXP) {
      null_to_na(as<List>(element));
    }
  }
}

// tranpose a nested list
// e.g. list(a=list(x=1,y=2,z=3), b=list(x=1,y=2,z=3)) =>
//        list(x=list(a=1,b=1), y=list(a=2,b=2), z=list(a=3,b=3))
// x is the list to tranpose
// output is a pre-allocated correctly-typed outputput placeholder
// type coercion is applied according to `output` type as necessary
// [[Rcpp::export]]
List transpose(List input, List output) {
  int row_count = input.size();
  if(row_count == 0) {
    return output;
  }

  int column_count = output.size();
  if(column_count == 0) {
    return output;
  }

  for (int c = 0; c < column_count; c++) {
    RObject column = output[c];
    switch(column.sexp_type()) {
    case VECSXP: { // list
      List list_column = as<List>(column);
      for (int r = 0; r < row_count; r++) {
        List row = input[r];
        RObject cell = row[c];
        if (!cell.isNULL()) {
          if(cell.sexp_type() == VECSXP) {
            null_to_na(as<List>(cell));
          }
          list_column[r] = cell;
        }
      }
      break;
    }
    case INTSXP: {
      // keep track whether we encounter a broken integer
      // integer can be broken because R only has 32-bit integers
      // where presto's BIGINT is 64-bit
      // NB: R is able to tap into floating number to faithfully represent
      //   up to 2^53
      bool broken_integer = false;
      // resulting vector if all integers are good
      IntegerVector integer_column = as<IntegerVector>(column);
      // placeholder vector if we need to coerce into numeric
      NumericVector numeric_column;

      for (int r = 0; r < row_count; r++) {
        List row = input[r];
        RObject cell = row[c];
        if (!cell.isNULL()) {
          if (broken_integer) {
            numeric_column[r] = as<double>(cell);
          } else {
            if (cell.sexp_type() == INTSXP) {
              integer_column[r] = as<int>(cell);
            } else {
              // first broken integer found. convert copied values to numerics
              // NB: this makes a copy and need to be reassigned in the end
              numeric_column = as<NumericVector>(integer_column);
              numeric_column[r] = as<double>(cell);
              broken_integer = true;
            }
          }
        }
      }

      // assign to numeric vector if we had to fallback
      if (broken_integer) {
        output[c] = numeric_column;
      }
      break;
    }
    case REALSXP: {
      // handle special cases
      NumericVector numeric_column = as<NumericVector>(column);
      for (int r = 0; r < row_count; r++) {
        List row = input[r];
        RObject cell = row[c];
        if (!cell.isNULL()) {
          if (cell.sexp_type() == STRSXP) {
            String cell_string = as<String>(cell);
            if (cell_string == "Infinity") {
              numeric_column[r] = INFINITY;
            } else if (cell_string == "-Infinity") {
              numeric_column[r] = -INFINITY;
            } else if (cell_string == "NaN") {
              numeric_column[r] = NAN;
            } else {
              stop("unrecognized string value in numeric vector!");
            }
          } else {
            numeric_column[r] = as<double>(cell);
          }
        }
      }
      break;
    }
    case LGLSXP: {
      copy_column<LogicalVector, bool>(as<LogicalVector>(column), row_count, input, c);
      break;
    }
    case STRSXP: {
      copy_column<CharacterVector, String>(as<CharacterVector>(column), row_count, input, c);
      break;
    }
    default: {
      stop("incompatible SEXP encountered!");
    }
    }
  }

  return output;
}


// You can include R code blocks in C++ files processed with sourceCpp
// (useful for testing and development). The R code will be automatically
// run after the compilation.
//

/*** R
transpose(list(), list())
transpose(list(list(1, "2"), list(3, "4")), list(c(NA_integer_, NA_integer_), c(NA_character_, NA_character_)))
*/
