# RPresto

RPresto is a [DBI](https://github.com/rstats-db/DBI)-based adapter for
the open source distributed SQL query engine [Presto](https://prestodb.io/)
for running interactive analytic queries.

## Installation

RPresto is not yet released on CRAN. You can install the github version via

```R
devtools::install_github('prestodb/RPresto')
```

## Examples

The standard DBI approach works with RPresto:

```R
con <- dbConnect(
  RPresto::Presto(),
  host='http://localhost',
  port=7777,
  user=Sys.getenv('USER'),
  schema='<schema>',
  catalog='<catalog>'
)

res <- dbSendQuery(con, 'SELECT 1')
# n != -1 for dbFetch is not supported
data <- dbFetch(res)

# For iris.sql()
source(system.file('tests', 'testthat', 'utilities.R', package='RPresto'))

iris <- dbGetQuery(con, paste("SELECT * FROM", iris.sql()))

dbDisconnect(con)
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

[1] See <https://gist.github.com/electrum/7710544> for an unofficial
description of the API.
