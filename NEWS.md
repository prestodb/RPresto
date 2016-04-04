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
