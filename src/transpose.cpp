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

void null_to_na(List x) {
  int size = x.size();
  for (int i = 0; i < size; i++) {
    SEXP elem = x[i];
    if (elem == R_NilValue) {
      x[i] = NA_LOGICAL;
    } else if(TYPEOF(elem) == VECSXP) {
      null_to_na(elem);
    }
  }
}

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
      copy<IntegerVector, int>(as<IntegerVector>(column), rows, x, c);
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
