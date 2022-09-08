
<!-- README.md is generated from README.Rmd. Please edit that file -->

# RPresto

<!-- badges: start -->
<!-- badges: end -->

RPresto is a [DBI](https://dbi.r-dbi.org/)-based adapter for the open
source distributed SQL query engine [Presto](https://prestodb.io/) for
running interactive analytic queries.

## Installation

RPresto is both on [CRAN](https://cran.r-project.org/package=RPresto)
and [github](https://github.com/prestodb/RPresto).

For the CRAN version, you can use

``` r
install.packages("RPresto")
```

You can install the development version of RPresto from
[GitHub](https://github.com/) with:

``` r
# install.packages("devtools")
devtools::install_github("prestodb/RPresto")
```

## Usage

**The following examples assume that you have a in-memory Presto server
set up locally.** It’s the simplest server which stores all data and
metadata in RAM on workers and both are discarded when Presto restarts.
If you don’t have one set up, please refer to the [memory connector
documentation](https://prestodb.io/docs/current/connector/memory.html).

``` r
# Load libaries and connect to Presto
library(RPresto)
library(DBI)

con <- DBI::dbConnect(
  drv = RPresto::Presto(),
  host = "http://localhost",
  port = 8080,
  user = Sys.getenv("USER"),
  catalog = "memory",
  schema = "default"
)
```

There are two levels of APIs: `DBI` and `dplyr`.

### `DBI` APIs

The easiest and most flexible way of executing a `SELECT` query is using
a [`dbGetQuery()`](https://dbi.r-dbi.org/reference/dbgetquery) call. It
returns the query result in a [`tibble`](https://tibble.tidyverse.org/).

``` r
DBI::dbGetQuery(con, "SELECT CAST(3.14 AS DOUBLE) AS pi")
#> # A tibble: 1 × 1
#>      pi
#>   <dbl>
#> 1  3.14
```

[`dbWriteTable()`](https://dbi.r-dbi.org/reference/dbwritetable) can be
used to write a small data frame into a Presto table.

``` r
# Writing iris data frame into Presto
DBI::dbWriteTable(con, "iris", iris)
```

[`dbExistsTable()`](https://dbi.r-dbi.org/reference/dbexiststable)
checks if a table exists.

``` r
DBI::dbExistsTable(con, "iris")
#> [1] TRUE
```

[`dbReadTable()`](https://dbi.r-dbi.org/reference/dbreadtable) reads the
entire table into R. It’s essentially a `SELECT *` query on the table.

``` r
DBI::dbReadTable(con, "iris")
#> # A tibble: 150 × 5
#>    sepal.length sepal.width petal.length petal.width species
#>           <dbl>       <dbl>        <dbl>       <dbl> <chr>  
#>  1          5.1         3.5          1.4         0.2 setosa 
#>  2          4.9         3            1.4         0.2 setosa 
#>  3          4.7         3.2          1.3         0.2 setosa 
#>  4          4.6         3.1          1.5         0.2 setosa 
#>  5          5           3.6          1.4         0.2 setosa 
#>  6          5.4         3.9          1.7         0.4 setosa 
#>  7          4.6         3.4          1.4         0.3 setosa 
#>  8          5           3.4          1.5         0.2 setosa 
#>  9          4.4         2.9          1.4         0.2 setosa 
#> 10          4.9         3.1          1.5         0.1 setosa 
#> # … with 140 more rows
```

[`dbRemoveTable()`](https://dbi.r-dbi.org/reference/dbremovetable) drops
the table from Presto.

``` r
DBI::dbRemoveTable(con, "iris")
```

You can execute a statement and returns the number of rows affected
using [`dbExecute()`](https://dbi.r-dbi.org/reference/dbexecute).

``` r
# Create an empty table using CREATE TABLE
DBI::dbExecute(
  con, "CREATE TABLE testing_table (field1 BIGINT, field2 VARCHAR)"
)
#> [1] 0
```

`dbExecute()` returns the number of rows affected by the statement.
Since a `CREATE TABLE` statement creates an empty table, it returns 0.

``` r
DBI::dbExecute(
  con,
  "INSERT INTO testing_table VALUES (1, 'abc'), (2, 'xyz')"
)
#> [1] 2
```

Since 2 rows are inserted into the table, it returns 2.

``` r
# Check the previous INSERT statment works
DBI::dbReadTable(con, "testing_table")
#> # A tibble: 2 × 2
#>   field1 field2
#>    <int> <chr> 
#> 1      1 abc   
#> 2      2 xyz
```

### `dplyr` APIs

We also include `dplyr` database backend integration (which is mainly
implemented using the [`dbplyr`
package](https://dbplyr.tidyverse.org/)).

``` r
# Load packages
library(dplyr)
library(dbplyr)
```

We can use
[`dplyr::copy_to()`](https://dplyr.tidyverse.org/reference/copy_to.html)
to write a local data frame to a `PrestoConnection` and immediately
create a remote table on it.

``` r
# Add mtcars to Presto
if (DBI::dbExistsTable(con, "mtcars")) {
  DBI::dbRemoveTable(con, "mtcars")
}
tbl.mtcars <- dplyr::copy_to(dest = con, df = mtcars, name = "mtcars")
# colnames() gives the column names
tbl.mtcars %>% colnames()
#>  [1] "mpg"  "cyl"  "disp" "hp"   "drat" "wt"   "qsec" "vs"   "am"   "gear"
#> [11] "carb"
```

[`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html) also
work directly on the `PrestoConnection`.

``` r
# Treat "iris" in Presto as a remote data source that dplyr can now manipulate
if (!DBI::dbExistsTable(con, "iris")) {
  DBI::dbWriteTable(con, "iris", iris)
}
tbl.iris <- dplyr::tbl(con, "iris")

tbl.iris %>% colnames()
#> [1] "sepal.length" "sepal.width"  "petal.length" "petal.width"  "species"

# dplyr verbs can be applied onto the remote data source
tbl.iris %>%
  group_by(species) %>%
  summarize(
    mean_sepal_length = mean(sepal.length, na.rm = TRUE)
  ) %>%
  arrange(species) %>%
  collect()
#> # A tibble: 3 × 2
#>   species    mean_sepal_length
#>   <chr>                  <dbl>
#> 1 setosa                  5.01
#> 2 versicolor              5.94
#> 3 virginica               6.59
```

## Connecting to Trino

To connect to Trino you must set the `use.trino.headers` parameter so
`RPresto` knows to send the correct headers to the server. Otherwise all
the same functionality is supported.

``` r
con.trino <- DBI::dbConnect(
  RPresto::Presto(),
  use.trino.headers=TRUE,
  host="http://localhost",
  port=7777,
  user=Sys.getenv("USER"),
  schema="<schema>",
  catalog="<catalog>",
  source="<source>"
)
```

## Passing extra credentials to the connector

To pass extraCredentials that gets added to the
`X-Presto-Extra-Credential` header use the `extra.credentials` parameter
so `RPresto` will add that to the header while creating the
`PrestoConnection`.

Set `use.trino.headers` if you want to pass extraCredentials through the
`X-Trino-Extra-Credential` header.

``` r
con <- DBI::dbConnect(
  RPresto::Presto(),
  host="http://localhost",
  port=7777,
  user=Sys.getenv("USER"),
  schema="<schema>",
  catalog="<catalog>",
  source="<source>",
  extra.credentials="test.token.foo=bar",
)
```

## How RPresto works

Presto exposes its interface via a REST based API[^1]. We utilize the
[httr](https://github.com/r-lib/httr) package to make the API calls and
use [jsonlite](https://github.com/jeroen/jsonlite) to reshape the data
into a `tibble`. Note that as of now, only read operations are
supported.

RPresto has been tested on Presto 0.100.

## License

RPresto is BSD-licensed.

[^1]: See <https://github.com/prestodb/presto/wiki/HTTP-Protocol> for a
    description of the API.
