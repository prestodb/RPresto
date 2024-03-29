% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/dbplyr-db.R
\name{db_list_tables.PrestoConnection}
\alias{db_list_tables.PrestoConnection}
\alias{db_has_table.PrestoConnection}
\alias{db_write_table.PrestoConnection}
\alias{db_copy_to.PrestoConnection}
\alias{db_compute.PrestoConnection}
\alias{db_sql_render.PrestoConnection}
\title{dbplyr database methods}
\usage{
\method{db_list_tables}{PrestoConnection}(con)

\method{db_has_table}{PrestoConnection}(con, table)

\method{db_write_table}{PrestoConnection}(
  con,
  table,
  types,
  values,
  temporary = FALSE,
  overwrite = FALSE,
  ...,
  with = NULL
)

\method{db_copy_to}{PrestoConnection}(
  con,
  table,
  values,
  overwrite = FALSE,
  types = NULL,
  temporary = TRUE,
  unique_indexes = NULL,
  indexes = NULL,
  analyze = TRUE,
  ...,
  in_transaction = TRUE,
  with = NULL
)

\method{db_compute}{PrestoConnection}(
  con,
  table,
  sql,
  temporary = TRUE,
  unique_indexes = list(),
  indexes = list(),
  analyze = TRUE,
  with = NULL,
  ...
)

\method{db_sql_render}{PrestoConnection}(con, sql, ..., use_presto_cte = TRUE)
}
\arguments{
\item{con}{A \code{PrestoConnection} as returned by \code{dbConnect()}.}

\item{table}{Table name}

\item{types}{Column types. If not provided, column types are inferred using
dbDataType.}

\item{values}{A \code{data.frame}.}

\item{temporary}{If a temporary table should be used. Not supported. Only
FALSE is accepted.}

\item{overwrite}{If an existing table should be overwritten.}

\item{...}{Extra arguments to be passed to individual methods.}

\item{with}{An optional WITH clause for the CREATE TABLE statement.}

\item{unique_indexes, indexes, analyze, in_transaction}{Ignored. Included
for compatibility with generics.}

\item{sql}{A SQL statement.}

\item{use_presto_cte}{\ifelse{html}{\href{https://lifecycle.r-lib.org/articles/stages.html#experimental}{\figure{lifecycle-experimental.svg}{options: alt='[Experimental]'}}}{\strong{[Experimental]}}
A logical value indicating if to use common table expressions stored in
PrestoConnection when possible. Default to TRUE. See
\code{vignette("common-table-expressions")}.}
}
\description{
dbplyr database methods
}
