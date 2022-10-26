#' Create dummy tables in Presto for testing
#'
#' @param con A valid `PrestoConnection` object.
#' @param table_name The resulting table name.
#' @param time_zone Time zone string for data types that require a time zone.
#'   Default to "America/New_York".
#' @param verbose Boolean indicating whether messages should be printed. Default
#'   to TRUE.
#' @name dummy_tables
#' @md
NULL

drop_table <- function(con, table_name, verbose = TRUE, ...) {
  if (DBI::dbExistsTable(con, table_name)) {
    DBI::dbRemoveTable(con, table_name)
  }
  if (verbose) {
    message("The previous table ", table_name, " has been dropped.")
  }
}

create_table <- function(con, table_name, sql, verbose = TRUE, ...) {
  dbCreateTableAs(con, table_name, sql)
  if (verbose) {
    message("The table ", table_name, " has been created.")
  }
}

#' Create a table that has an ARRAY of 3 elements for all primitive data types
#'
#' [create_primitive_arrays_table()] creates a dummy
#' table that has ARRAYs of primitive Presto data types.
#'
#' We construct the **arrays-of-primitive-types** table by putting two different
#' values of the same type and a NULL value in an array. In this way, the three
#' values of the same type appear together in the source code and therefore are
#' easier to compare. For integer values, we use the theoretical lower bound
#' (i.e., minimum value) and the theoretical upper bound (i.e., maximum value)
#' as the two values. The field names are taken from the Presto data types they
#' represent.
#'
#' @details
#' Here's the complete primitive type values included in the table
#'
#' | Index | Column | Type | ARRAY values |
#' |-------|--------|------|--------------|
#' | 1 | boolean | BOOLEAN | \[true, false, null\] |
#' | 2 | tinyint | TINYINT | \[-128, 127, null\] |
#' | 3 | smallint | SMALLINT | \[-32768, 32767, null\] |
#' | 4 | integer | INTEGER | \[-2147483647, 2147483647, null\] |
#' | 5 | bigint | BIGINT | \[-9007199254740991, 9007199254740991, null\] |
#' | 6 | real | REAL | \[1.0, 2.0, null\] |
#' | 7 | double | DOUBLE | \[1.0, 2.0, null\] |
#' | 8 | decimal | DECIMAL | \[-9007199254740991.5, 9007199254740991.5, null\] |
#' | 9 | varchar | VARCHAR | \['abc', 'def', null\] |
#' | 10 | char | CHAR | \['a', 'b', null\] |
#' | 11 | varbinary | VARBINARY | \['abc', 'def', null\] |
#' | 12 | date | DATE | \['2000-01-01', '2000-01-02', null\] |
#' | 13 | time | TIME | \['01:02:03.456', '02:03:04.567', null\] |
#' | 14 | time_with_tz | TIME WITH TIME ZONE | \['01:02:03.456 \<tz\>', '02:03:04.567 \<tz\>', null\] |
#' | 15 | timestamp | TIMESTAMP | \['2000-01-01 01:02:03.456', '2000-01-02 02:03:04.567', null\] |
#' | 16 | timestamp_with_tz | TIMESTAMP WITH TIME ZONE | \['2000-01-01 01:02:03.456 \<tz\>', '2000-01-02 02:03:04.567 \<tz\>', null\] |
#' | 17 | interval_year_to_month | INTERVAL YEAR TO MONTH | \['14' MONTH, '28' MONTH, null\] |
#' | 18 | interval_day_to_second | INTERVAL DAY TO SECOND | \['2 4:5:6.500' DAY TO SECOND, '3 7:8:9.600' DAY TO SECOND, null\] |
#' @keywords internal
#' @rdname dummy_tables
#' @md
create_primitive_arrays_table <- function(con,
                                          table_name = "presto_primitive_arrays",
                                          time_zone = "America/New_York",
                                          verbose = TRUE) {
  drop_table(con, table_name, verbose)
  create_table_sql <- paste0("
    SELECT
      ARRAY[
        true,
        false,
        null
      ] AS array_boolean,
      ARRAY[
        CAST('-128' AS TINYINT),
        CAST('127' AS TINYINT),
        CAST(null AS TINYINT)
      ] AS array_tinyint,
      ARRAY[
        CAST('-32768' AS SMALLINT),
        CAST('32767' AS SMALLINT),
        CAST(null AS SMALLINT)
      ] AS array_smallint,
      ARRAY[
        CAST('-2147483647' AS INTEGER),
        CAST('2147483647' AS INTEGER),
        CAST(null AS INTEGER)
      ] AS array_integer,
      ARRAY[
        CAST('-9007199254740991' AS BIGINT),
        CAST('9007199254740991' AS BIGINT),
        CAST(null AS BIGINT)
      ] AS array_bigint,
      ARRAY[
        CAST(1.0 AS REAL),
        CAST(2.0 AS REAL),
        CAST(null AS REAL)
      ] AS array_real,
      ARRAY[
        CAST(1.0 AS DOUBLE),
        CAST(2.0 AS DOUBLE),
        CAST(null AS DOUBLE)
      ] AS array_double,
      ARRAY[
        CAST(-9007199254740991.5 AS DECIMAL(17,1)),
        CAST(9007199254740991.5 AS DECIMAL(17,1)),
        CAST(null AS DECIMAL(17,1))
      ] AS array_decimal,
      ARRAY[
        CAST('abc' AS VARCHAR),
        CAST('def' AS VARCHAR),
        CAST(null AS VARCHAR)
      ] AS array_varchar,
      ARRAY[
        CAST('a' AS CHAR),
        CAST('b' AS CHAR),
        CAST(null AS CHAR)
      ] AS array_char,
      ARRAY[
        CAST('abc' AS VARBINARY),
        CAST('def' AS VARBINARY),
        CAST(null AS VARBINARY)
      ] AS array_varbinary,
      ARRAY[
        DATE '2000-01-01',
        DATE '2000-01-02',
        null
      ] AS array_date,
      ARRAY[
        TIME '01:02:03.456',
        TIME '02:03:04.567',
        null
      ] AS array_time,
      ARRAY[
        TIME '01:02:03.456 ", time_zone, "',
        TIME '02:03:04.567 ", time_zone, "',
        null
      ] AS array_time_with_tz,
      ARRAY[
        TIMESTAMP '2000-01-01 01:02:03.456',
        TIMESTAMP '2000-01-02 02:03:04.567',
        null
      ] AS array_timestamp,
      ARRAY[
        TIMESTAMP '2000-01-01 01:02:03.456 ", time_zone, "',
        TIMESTAMP '2000-01-02 02:03:04.567 ", time_zone, "',
        null
      ] AS array_timestamp_with_tz,
      ARRAY[
        INTERVAL '14' MONTH,
        INTERVAL '28' MONTH,
        null
      ] AS array_interval_year_to_month,
      ARRAY[
        INTERVAL '2 4:5:6.500' DAY TO SECOND,
        INTERVAL '3 7:8:9.600' DAY TO SECOND,
        null
      ] AS array_interval_day_to_second
    ")
  create_table(con, table_name, create_table_sql, verbose)
  invisible(TRUE)
}

#' Create a table that has a MAP of 3 elements for all primitive data types
#'
#' [create_primitive_maps_table()] creates a dummy table that has
#' MAPs of primitive Presto data types.
#'
#' We construct the **maps-of-primitive-types** table by first creating a table
#' with ARRAYs of all primitive data types. We then use the MAP() function to
#' create the MAPs from ARRAYs.
#'
#' @keywords internal
#' @rdname dummy_tables
#' @md
create_primitive_maps_table <- function(con,
                                        table_name = "presto_primitive_maps",
                                        time_zone = "America/New_York",
                                        verbose = TRUE) {
  drop_table(con, table_name, verbose)
  tmp_table_name <- paste0("tmp_pretso_primitive_arrays")
  create_primitive_arrays_table(
    con, tmp_table_name, time_zone, verbose
  )
  create_table_sql <- paste0("
    SELECT
      MAP(ARRAY[1, 2, 3], array_boolean) AS map_boolean,
      MAP(ARRAY[1, 2, 3], array_tinyint) AS map_tinyint,
      MAP(ARRAY[1, 2, 3], array_smallint) AS map_smallint,
      MAP(ARRAY[1, 2, 3], array_integer) AS map_integer,
      MAP(ARRAY[1, 2, 3], array_bigint) AS map_bigint,
      MAP(ARRAY[1, 2, 3], array_real) AS map_real,
      MAP(ARRAY[1, 2, 3], array_double) AS map_double,
      MAP(ARRAY[1, 2, 3], array_decimal) AS map_decimal,
      MAP(ARRAY[1, 2, 3], array_varchar) AS map_varchar,
      MAP(ARRAY[1, 2, 3], array_char) AS map_char,
      MAP(ARRAY[1, 2, 3], array_varbinary) AS map_varbinary,
      MAP(ARRAY[1, 2, 3], array_date) AS map_date,
      MAP(ARRAY[1, 2, 3], array_time) AS map_time,
      MAP(ARRAY[1, 2, 3], array_time_with_tz) AS map_time_with_tz,
      MAP(ARRAY[1, 2, 3], array_timestamp) AS map_timestamp,
      MAP(ARRAY[1, 2, 3], array_timestamp_with_tz) AS map_timestamp_with_tz,
      MAP(ARRAY[1, 2, 3], array_interval_year_to_month)
        AS map_interval_year_to_month,
      MAP(ARRAY[1, 2, 3], array_interval_day_to_second)
        AS map_interval_day_to_second
    FROM ", tmp_table_name)
  create_table(con, table_name, create_table_sql, verbose)
  drop_table(con, tmp_table_name, verbose)
  invisible(TRUE)
}

#' Create a table that has 3 rows of all primitive Presto data types with dummy
#' data
#'
#' [create_primitive_types_table()] creates a dummy table that has
#' primitive Presto data types.
#'
#' We construct the **primitive-types** table by first creating a table with
#' ARRAYs of all primitive data types. We then use Presto's UNNEST() function to
#' expand the arrays into three separate rows. Each supported Presto data type
#' has three rows in the table so that the resulting R data frame is distinctly
#' different from a simple named list.
#'
#' @keywords internal
#' @rdname dummy_tables
#' @md
create_primitive_types_table <- function(con,
                                         table_name = "presto_primitive_types",
                                         time_zone = "America/New_York",
                                         verbose = TRUE) {
  drop_table(con, table_name, verbose)
  tmp_table_name <- paste0("tmp_pretso_primitive_arrays")
  create_primitive_arrays_table(
    con, tmp_table_name, time_zone, verbose
  )
  create_table_sql <- paste0("
    SELECT
      boolean,
      tinyint,
      smallint,
      integer,
      bigint,
      real,
      double,
      decimal,
      varchar,
      char,
      varbinary,
      date,
      time,
      time_with_tz,
      timestamp,
      timestamp_with_tz,
      interval_year_to_month,
      interval_day_to_second
    FROM ", tmp_table_name, " AS foo
    CROSS JOIN
    UNNEST(
      array_boolean,
      array_tinyint,
      array_smallint,
      array_integer,
      array_bigint,
      array_real,
      array_double,
      array_decimal,
      array_varchar,
      array_char,
      array_varbinary,
      array_date,
      array_time,
      array_time_with_tz,
      array_timestamp,
      array_timestamp_with_tz,
      array_interval_year_to_month,
      array_interval_day_to_second
    ) t(
      boolean,
      tinyint,
      smallint,
      integer,
      bigint,
      real,
      double,
      decimal,
      varchar,
      char,
      varbinary,
      date,
      time,
      time_with_tz,
      timestamp,
      timestamp_with_tz,
      interval_year_to_month,
      interval_day_to_second
    )
    ")
  create_table(con, table_name, create_table_sql, verbose)
  drop_table(con, tmp_table_name, verbose)
  invisible(TRUE)
}

#' Create a table that has 1 ROW column to include all primitive types
#'
#' [create_primitive_rows_table()] creates a dummy table that has all primitive
#' data types included in one `ROW` type column.
#'
#' We construct the **primitive-rows** table by first creating a table with
#' all primitive data types. We then use Presto's `CAST(ROW() AS ROW())`
#' function to create the `ROW` column.
#'
#' @keywords internal
#' @rdname dummy_tables
#' @md
create_primitive_rows_table <- function(con,
                                        table_name = "presto_primitive_rows",
                                        time_zone = "America/New_York",
                                        verbose = TRUE) {
  drop_table(con, table_name, verbose)
  tmp_table_name <- paste0("tmp_pretso_primitive_types")
  create_primitive_types_table(
    con, tmp_table_name, time_zone, verbose
  )
  create_table_sql <- paste0("
    SELECT
      CAST(
        ROW(
          boolean,
          tinyint,
          smallint,
          integer,
          bigint,
          real,
          double,
          decimal,
          varchar,
          char,
          varbinary,
          date,
          time,
          time_with_tz,
          timestamp,
          timestamp_with_tz,
          interval_year_to_month,
          interval_day_to_second
        ) AS
        ROW(
          boolean BOOLEAN,
          tinyint TINYINT,
          smallint SMALLINT,
          integer INTEGER,
          bigint BIGINT,
          real REAL,
          double DOUBLE,
          decimal DECIMAL(17,1),
          varchar VARCHAR,
          char CHAR,
          varbinary VARBINARY,
          date DATE,
          time TIME,
          time_with_tz TIME WITH TIME ZONE,
          timestamp TIMESTAMP,
          timestamp_with_tz TIMESTAMP WITH TIME ZONE,
          interval_year_to_month INTERVAL YEAR TO MONTH,
          interval_day_to_second INTERVAL DAY TO SECOND
        )
      ) AS row_primitive_types
    FROM ", tmp_table_name)
  create_table(con, table_name, create_table_sql, verbose)
  drop_table(con, tmp_table_name, verbose)
  invisible(TRUE)
}

#' Create a table that has an `ARRAY(ROW)` column that has 2 `ROW` elements in
#' an `ARRAY`. Each `ROW` element has all 17 supported primitive data types.
#'
#' [create_array_of_rows_table()] creates a dummy table that has an `ARRAY(ROW)`
#' column that has 2 `ROW` elements, each containing all 17 supported primitive
#' data types.
#'
#' We construct the **array-of-rows** table by first creating a table with
#' a `ROW` type column that includes all 17 supported primitive data types. We
#' then use the `ARRAY[]` function to construct the 2-element `ARRAY(ROW)`
#' column.
#'
#' @keywords internal
#' @rdname dummy_tables
#' @md
create_array_of_rows_table <- function(con,
                                       table_name = "presto_array_of_rows",
                                       time_zone = "America/New_York",
                                       verbose = TRUE) {
  drop_table(con, table_name, verbose)
  tmp_table_name <- paste0("tmp_pretso_primitive_rows")
  create_primitive_rows_table(
    con, tmp_table_name, time_zone, verbose
  )
  create_table_sql <- paste0("
    SELECT ARRAY[row_primitive_types, row_primitive_types] AS array_of_rows
    FROM ", tmp_table_name)
  create_table(con, table_name, create_table_sql, verbose)
  drop_table(con, tmp_table_name, verbose)
  invisible(TRUE)
}

#' Create a table that has an `ARRAY(MAP)` column that has 2 `MAP` elements in
#' an `ARRAY`.
#'
#' [create_array_of_maps_table()] creates a dummy table that has 17 `ARRAY(MAP)`
#' columns, each of which has an `ARRAY` of 2 `MAP` elements.
#'
#' We construct the **array-of-maps** table by first creating a table a
#' primitive `MAP` table and then calling the `ARRAY[]` function to create the
#' `ARRAY(MAP)` columns.
#'
#' @keywords internal
#' @rdname dummy_tables
#' @md
create_array_of_maps_table <- function(con,
                                       table_name = "presto_array_of_maps",
                                       time_zone = "America/New_York",
                                       verbose = TRUE) {
  drop_table(con, table_name, verbose)
  tmp_table_name <- paste0("tmp_pretso_primitive_maps")
  create_primitive_maps_table(
    con, tmp_table_name, time_zone, verbose
  )
  create_table_sql <- paste0("
    SELECT
      ARRAY[map_boolean, map_boolean] AS array_map_boolean,
      ARRAY[map_tinyint, map_tinyint] AS array_map_tinyint,
      ARRAY[map_smallint, map_smallint] AS array_map_smallint,
      ARRAY[map_integer, map_integer] AS array_map_integer,
      ARRAY[map_bigint, map_bigint] AS array_map_bigint,
      ARRAY[map_real, map_real] AS array_map_real,
      ARRAY[map_double, map_double] AS array_map_double,
      ARRAY[map_decimal, map_decimal] AS array_map_decimal,
      ARRAY[map_varchar, map_varchar] AS array_map_varchar,
      ARRAY[map_char, map_char] AS array_map_char,
      ARRAY[map_varbinary, map_varbinary] AS array_map_varbinary,
      ARRAY[map_date, map_date] AS array_map_date,
      ARRAY[map_time, map_time] AS array_map_time,
      ARRAY[map_time_with_tz, map_time_with_tz] AS array_map_time_with_tz,
      ARRAY[map_timestamp, map_timestamp] AS array_map_timestamp,
      ARRAY[map_timestamp_with_tz, map_timestamp_with_tz]
        AS array_map_timestamp_with_tz,
      ARRAY[map_interval_year_to_month, map_interval_year_to_month]
        AS array_map_interval_year_to_month,
      ARRAY[map_interval_day_to_second, map_interval_day_to_second]
        AS array_map_interval_day_to_second
    FROM ", tmp_table_name)
  create_table(con, table_name, create_table_sql, verbose)
  drop_table(con, tmp_table_name, verbose)
  invisible(TRUE)
}
