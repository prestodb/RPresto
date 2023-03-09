# RPresto 1.4.4

* Fixed error whereby `compute()` returns a "table doesn't exist" error when the
  computed table contains CTE. (#243)
* Hotfix to update `tbl()` and `db_query_rows()` args to fix warnings by R CMD
  CHECK on S3 generic consistency raised by CRAN. (#245)

# RPresto 1.4.3

* Fixed `dbplyr` 2.3.0 compatibility issues. (#237)

# RPresto 1.4.2

* Add a convenient wrapper `kerberos_configs()` to generate Kerberos header
  configs that can be passed to the `request.config` argument of `dbConnect()`.
  (#202 and #221). Thanks to @suzzettedetorres for providing this solution.
* All functions that take table name as argument now work with
  `dbplyr::in_schema()` and `DBI::Id()` besides the usual character table name.
  (#228)
* Create a new `dbRenameTable()` function to rename table name.

# RPresto 1.4.1

* `dbListFields` now works with identifier name which in turn supports the use
  of `in_schema()` in `tbl()` (#200)
* Fix an error whereby join query's CTEs are not properly retrieved (#203)
* Fix a bug that causes NULL values in a ROW to return data-schema mismatch
  error (#206)
* Add `dbQuoteLiteral()` and `dbAppendTable()` implementations.
* Rewrite `dbWriteTable()` implementation to use `dbCreateTable()` and
  `dbAppendTable()`. It also supports all default arguments now (#199)
* `dbWriteTable()` gains a `use.one.query` option to use a single `CREATE TABLE
  AS` query.

# RPresto 1.4.0

* Change maintainer to Jarod Meng (jarodm@fb.com)
* Major refactoring of the internal schema and data parsing functions to enable
more robust mapping between Presto data types and R types
  * The output is changed from `data.frame` to `tibble` to offer better printing
    and be more consistent with other DBI-compatible datawarehouse packages
  * Add more user-friendly R types translation for primitive Presto data types
    (e.g., DATE types are now translated to `Date` classes in R; TIMESTAMP types
    are translated to `POSIXct` classes; TIME types are translated to `difftime`
    classes; INTERVAL types are translated to `Duration` classes)
  * Enable more choices of BIGINT type handling (i.e., `integer64`, `integer`,
    `numeric`, or `character`). (#61)
  * Add complete support for complex Presto types (i.e., ARRAY, MAP, and ROW).
    They are now translated to typed vectors, lists, or tibbles depending on the
    types and structure of the data. (#118)
  * It supports all primitive and complex data types for Trino too. (#176)
* Add vignettes on Presto-R type translations (see `vignette("primitive-types")`
  and `vignette("complex-types")`)
* `dbExistsTable()` error when quoted identifier is supplied as `name` is fixed
  (#167)
* Add a few `dplyr` and `dbplyr` method implementations. See
  `backend-implementation.md` for the details.
  * `dbplyr::sql_query_save()`
  * `dplyr::db_list_tables()`
  * `dplyr::db_has_table()`
  * `dplyr::db_write_table()`
  * `dbplyr::db_copy_to()`
  * `dplyr::copy_to()` method for `src_presto` and `PrestoConnection`
  * `dplyr::tbl()` method for `PrestoConnection`
  * `dbplyr::db_compute()`
  * `dplyr::compute()`
* `PrestoConnection` gains a `request.config` slot whereby users can set extra
  Curl configs (as returned by `httr::config()` to GET/POST requests. (#173)
* Styling the whole package using `styler::style_pkg()`. A notable change is to
  user double quotes everywhere instead of a combination of single and double
  quotes. (#174)
* Add an experimental feature to support common table expressions (CTEs) in both
  DBI and dplyr backends (see `vignette("common-table-expressions")`) (#175)
* `dbConnect()` now uses empty string "" (rather than UTC) as the default
  session.timezone which translates to the Presto server timezone.
* `dbConnect()` and `src_presto()` gain an `output.timezone` argument which can
  be used to control how `TIME WITH TZ` and `TIMESTAMP` values are represented
  in the output tibble.
* `dbGetQuery()` and `dbSendQuery()` gain a `quiet` argument which defaults to
  `getOption("rpresto.quiet")` which is `NA` if not set. (#191)

# RPresto 1.3.8

- Fix failing unit tests (#141)
- Support Trino headers in session (#143)
- Update copyright headers
- Migrate RPresto's dplyr interface to use dbplyr 2.0.0 backend (#150)
* Add documentation on RPresto's DBI and `dplyr` backend implementation

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
