#include <Rcpp.h>
using namespace Rcpp;

// copy from x with number of rows, to c column in columns
template <typename T, class RTYPE>
inline void copy(T column, int rows, List x, int c) {
  for (int r = 0; r < rows; r++) {
    List row = x[r];
    SEXP cell = row[c];
    if (cell != R_NilValue) {
      column[r] = as<RTYPE>(cell);
    }
  }
}

// set null element in list to NA (NA_LOGICAL) recursively
void null_to_na(List x) {
  int size = x.size();
  for (int i = 0; i < size; i++) {
    SEXP elem = x[i];
    if (elem == R_NilValue) {
      x[i] = LogicalVector::create(NA_LOGICAL);
    } else if(TYPEOF(elem) == VECSXP) {
      null_to_na(elem);
    }
  }
}

// tranpose a nested list
// e.g. list(a=list(x=1,y=2,z=3), b=list(x=1,y=2,z=3)) =>
//        list(x=list(a=1,b=1), y=list(a=2,b=2), z=list(a=3,b=3))
// x is the list to tranpose
// out is a pre-allocated correctly-typed output placeholder
// type coercion is applied according to `out` type as necessary
// [[Rcpp::export(name = ".transpose")]]
List transpose(List x, List out) {
  int rows = x.size();
  if(rows == 0) return out;

  int cols = out.size();
  if(cols == 0) return out;

  for (int c = 0; c < cols; c++) {
    SEXP column = VECTOR_ELT(out, c);
    switch(TYPEOF(column)) {
    case VECSXP: { // list
      for (int r = 0; r < rows; r++) {
        SEXP row = VECTOR_ELT(x, r);
        SEXP cell = VECTOR_ELT(row, c);
        if (cell != R_NilValue) {
          if(TYPEOF(cell) == VECSXP) {
            null_to_na(cell);
          }
          SET_VECTOR_ELT(column, r, cell);
        }
      }
      break;
    }
    case INTSXP: {
      // keep track whether we encounter a broken integer
      bool broken_int = false;
      // resulting vector if all integers are good
      IntegerVector intColumn = as<IntegerVector>(column);
      // placeholder vector if we need to coerce into numerics
      NumericVector numColumn;

      for (int r = 0; r < rows; r++) {
        List row = x[r];
        SEXP cell = row[c];
        if (cell != R_NilValue) {
          // keep populating integer vector
          if (!broken_int && TYPEOF(cell) == INTSXP) {
            intColumn[r] = as<int>(cell);
          } else {
            // fallback current integer vector to numeric vector
            if (!broken_int) {
              numColumn = as<NumericVector>(intColumn);
              broken_int = true;
            }
            numColumn[r] = as<double>(cell);
          }
        }
      }

      // set to numeric vector for bigint coercion
      out[c] = broken_int ? numColumn : intColumn;
      break;
    }
    case REALSXP: {
      // handle special cases
      NumericVector tmp = as<NumericVector>(column);
      for (int r = 0; r < rows; r++) {
        List row = x[r];
        SEXP cell = row[c];
        if (cell != R_NilValue) {
          if (TYPEOF(cell) == STRSXP) {
            String cellStr = as<String>(cell);
            if (cellStr == "Infinity") {
              tmp[r] = INFINITY;
            } else if (cellStr == "-Infinity") {
              tmp[r] = -INFINITY;
            } else if (cellStr == "NaN") {
              tmp[r] = NAN;
            } else {
              stop("unrecognized string value in numeric vector!");
            }
          } else {
            tmp[r] = as<double>(cell);
          }
        }
      }
      break;
    }
    case LGLSXP: {
      copy<LogicalVector, bool>(as<LogicalVector>(column), rows, x, c);
      break;
    }
    case STRSXP: {
      copy<CharacterVector, String>(as<CharacterVector>(column), rows, x, c);
      break;
    }
    default: {
      stop("incompatible SEXP encountered!");
    }
    }
  }

  return out;
}


// You can include R code blocks in C++ files processed with sourceCpp
// (useful for testing and development). The R code will be automatically
// run after the compilation.
//

/*** R
.transpose(list(), list())
.transpose(list(list(1, "2"), list(3, "4")), list(c(NA_integer_, NA_integer_), c(NA_character_, NA_character_)))
*/
