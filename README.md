# RPresto

RPresto is a [DBI](https://github.com/rstats-db/DBI)-based adapter for
the open source distributed SQL query engine [Presto](https://prestodb.io/)
for running interactive analytic queries.

## Installation

RPresto is both on [CRAN](https://cran.r-project.org/package=RPresto)
and [github](https://github.com/prestodb/RPresto).
For the CRAN version, you can use

```R
install.packages('RPresto')
```

You can install the github development version via

```R
devtools::install_github('prestodb/RPresto')
```

## Examples

The standard DBI approach works with RPresto:

```R
library('DBI')

con <- dbConnect(
  RPresto::Presto(),
  host='http://localhost',
  port=7777,
  user=Sys.getenv('USER'),
  schema='<schema>',
  catalog='<catalog>',
  source='<source>'
)

res <- dbSendQuery(con, 'SELECT 1')
# dbFetch without arguments only returns the current chunk, so we need to
# loop until the query completes.
while (!dbHasCompleted(res)) {
    chunk <- dbFetch(res)
    print(chunk)
}

res <- dbSendQuery(con, 'SELECT CAST(NULL AS VARCHAR)')
# Due to the unpredictability of chunk sizes with presto, we do not support
# custom number of rows
# testthat::expect_error(dbFetch(res, 5))

# To get all rows using dbFetch, pass in a -1 argument
print(dbFetch(res, -1))

# An alternative is to use dbGetQuery directly

# `source` for iris.sql()
source(system.file('tests', 'testthat', 'utilities.R', package='RPresto'))

iris <- dbGetQuery(con, paste("SELECT * FROM", iris.sql()))

dbDisconnect(con)
```

We also include [dplyr](https://github.com/hadley/dplyr) integration.

```R
library(dplyr)

db <- src_presto(
  host='http://localhost',
  port=7777,
  user=Sys.getenv('USER'),
  schema='<schema>',
  catalog='<catalog>',
  source='<source>'
)

# Assuming you have a table like iris in the database
iris <- tbl(db, 'iris')

iris %>%
  group_by(species) %>%
  summarise(mean_sepal_length = mean(as(sepal_length, 0.0))) %>%
  arrange(species) %>%
  collect()
```

## How RPresto works

Presto exposes its interface via a REST based API<sup>1</sup>. We utilize the
[httr](https://github.com/hadley/httr) package to make the API calls and
use [jsonlite](https://github.com/jeroenooms/jsonlite) to reshape the
data into a `data.frame`. Note that as of now, only read operations are
supported.

RPresto has been tested on Presto 0.100.

## License
RPresto is BSD-licensed. We also provide an additional patent grant.

[1] See <https://github.com/prestodb/presto/wiki/HTTP-Protocol> for a
description of the API.
