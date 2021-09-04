# RPresto 1.3.7

- Fix testing errors caused by Presto changes since last update (#131)
- Change `[[` translation from `[]` subscript operator to `ELEMENT_AT()` (#132)
- Enable simple ROW type support (#137)
- Fix a bug whereby `is.infinite()` is incorrectly translated to `IS_FINITE()`
  instead of `IS_INFINITE()` in SQL (#139)
- Disabled translation of `median()` and `quantile()` and suggested `approx_quantile()` instead. (#120)

# RPresto 1.3.6

- Change maintainer to Thomas Leeper (thomasleeper@fb.com)

# RPresto 1.3.5

- Add custom Date and POSIXct sql translation implementations for dbplyr (#123, thanks to @OssiLehtinen for original implementations).
- Adapt `dbClearResult` to the API change, we now need to `DELETE` `/v1/query/<query_id>`.
- Add a `query.id` slot to `PrestoResult`.

# RPresto 1.3.4

- Translate `[[` to allow indexing arrays and maps with dplyr (#110).
- Switch from BSD+Patents license to Standard BSD license (#114).

# RPresto 1.3.3

- Fix tests for compatibility with `dbplyr` 1.4.0.
- Send headers with all http requests (#103).
- Add support for `as.<data_type>` style casting for dplyr (#97).
- Add Code of Conduct.

# RPresto 1.3.2

- Use the new delayed S3 registration mechanism in 3.6.0 for dplyr method implementations.
- Bump minimum dplyr version required to 0.7.0.

# RPresto 1.3.1

- Presto now might return data in POST response (#89)
- Presto now might not always return column information in each response (#93)
- Better error message for unknown column types (#86, #87)
- Adapt to presto changes for type casting translation (#88)
- Add CHAR data type (#91)

# RPresto 1.3.0

- Fix Rcpp compilation under Windows (#79)
- `SET/RESET SESSION` queries are now correctly respected when used under `dbGetQuery`.
  `PrestoConnection` no longer has `parameters` slot but `dbConnect` remains backward
  compatible. Manual change to parameter is still possible via `conn@session$setParameter()` (#77)
- Adapt to changes in dplyr version 0.7.0, mainly around the remote
  backend support being split to `dbplyr`. This should be backwards compatible
  back to dplyr 0.4.3 (#76)
- Add support for the REAL data type (#70)
- Allow specifying the connection source (#68)
- Drop RCurl dependency (#67)
- Return DECIMAL data types as characters as opposed to numeric's
  previously (#64, fixes #60)
- Add support for new integer data types (INTEGER, SMALLINT, TINYINT) (#59, fixes #56)
- Migrate the  `json` to `data.frame` construction from pure R to Rcpp for 10x
  speed gains! (thanks @saurfang) (#57, #58)
- Fix dbListFields to use the nextUri instead of infoUri (#55)
- Don't drop data for duplicate column names (#53)

# RPresto 1.2.1

- Handle responses with no column information (fixes #49)
- Add retries for GET and POST responses with error status codes
- Skip test cases for ones that need locale modification if we cannot set the locale for the OS.
- Adapt to changes in the upcoming dplyr and testthat versions.

# RPresto 1.2.0

- Add a `session.timezone` parameter to `dbConnect` and `src_presto` which
  defaults to UTC.  This affects the timestamps returned for Presto data types
  "TIMESTAMP".  We handle the ambiguity by assigning a time zone to _every_
  POSIXct column returned. Note that if you are doing `as.character()` directly
  on these columns, the values you obtain will be different from what happened
  before.
- Fix the way we handle zero row multiple column query results. This will
  affect `LIMIT 0` queries specifically.

# RPresto 1.1.1

- Minor dplyr related fixes
- Drop the R version requirement from 3.1.1 to 3.1.0
- Speed-up in binding chunks if dplyr is available.
- Handle special values like Infinity, NaN.

# RPresto 1.1.0

- Add optional dplyr support. One can initiate a connection via `src_presto`.
- Minor documentation fixes.

# RPresto 1.0.0

- Initial release to github
