# RPresto's implmentation of DBI and `dplyr` backends

There are two main layers of RPresto's code: a
[DBI backend](https://dbi.r-dbi.org/) and a
[`dplyr` remote database backend](https://dbplyr.tidyverse.org/articles/new-backend.html)
mainly using [`dbplyr`](https://dbplyr.tidyverse.org/).

## `DBI` backend

Important classes:

| Class | Implementation type | File |
| ----- | ------------------- | ---- |
| PrestoDriver| S4 | PrestoDriver.R |
| PrestoConnection | S4 | PrestoConnection.R |
| PrestoResult | S4 | PrestoResult.R |
| PrestoQuery | RefClass | PrestoQuery.R |
| PrestoSession | RefClass | PrestoSession.R |

Important methods:

| Method | Class | Status | File |
| ------ | ----- | ------ | ---- |
| dbGetInfo | PrestoDriver | Implemented | dbGetInfo.R |
| dbDataType | PrestoDriver | Implemented | dbDataType.R |
| dbUnloadDriver | PrestoDriver | Implemented | dbUnloadDriver.R |
| dbConnect | PrestoDriver | Implemented | dbConnect.R |
| dbGetInfo | PrestoConnection | Implemented | dbGetInfo.R |
| dbDisconnect | PrestoConnection | Implemented | dbDisconnect.R |
| dbQuoteIdentifier | PrestoConnection | Partially implemented | dbQuoteIdentifier.R |
| dbUnquoteIdentifier | PrestoConnection | Default | |
| dbQuoteString | PrestoConnection | Default | |
| dbQuoteLiteral | PrestoConnection | Implemented | dbQuoteLiteral.R |
| dbListTables | PrestoConnection | Implemented | dbListTables.R |
| dbExistsTable | PrestoConnection | Implemented | dbExistsTable.R |
| dbSendQuery | PrestoConnection | Implemented | dbSendQuery.R |
| dbSendStatement | PrestoConnection | Default | |
| dbGetQuery | PrestoConnection | Implemented | dbGetQuery.R |
| dbExecute | PrestoConnection | Default | |
| dbCreateTable | PrestoConnection | Implemented | dbCreateTable.R |
| sqlCreateTable | PrestoConnection | Implemented | sqlCreateTable.R |
| dbCreateTableAs | PrestoConnection | Created | dbCreateTableAs.R |
| sqlCreateTableAs | PrestoConnection | Created | sqlCreateTableAs.R |
| dbAppendTableAs | PrestoConnection | Created | dbAppendTableAs.R |
| sqlAppendTableAs | PrestoConnection | Created | sqlAppendTableAs.R |
| dbWriteTable | PrestoConnection | Implemented | dbWriteTable.R |
| dbRemoveTable | PrestoConnection | Implemented | dbRemoveTable.R |
| dbReadTable | PrestoConnection | Implemented | dbReadTable.R |
| dbRenameTable | PrestoConnection | Created | dbRenameTable.R |
| dbAppendtable | PrestoConnection | Implemented | dbAppendTable.R |
| dbListFields | PrestoConnection | Implemented | dbListFields.R |
| dbBegin | PrestoConnection | Not implemented | |
| dbCommit | PrestoConnection | Not implemented | |
| dbRollback | PrestoConnection | Not implemented | |
| dbBreak | PrestoConnection | Not implemented | |
| dbWithTransaction | PrestoConnection | Not implemented | |
| dbGetInfo | PrestoResult | Implemented | dbGetInfo.R |
| dbClearResult | PrestoResult | Implemented | dbClearResult.R |
| dbFetch | PrestoResult | Implemented | dbFetch.R |
| fetch | PrestoResult | Implemented | fetch.R |
| dbHasCompleted | PrestoResult | Implemented | dbHasCompleted.R |
| dbIsValid | PrestoResult | Implemented | dbIsValid.R |
| dbGetStatement | PrestoResult | Implemented | dbGetStatement.R |
| dbGetRowCount | PrestoResult | Implemented | dbGetRowCount.R |
| dbGetRowsAffected | PrestoResult | Implemented | dbGetRowsAffected.R |
| dbListFields | PrestoResult | Implemented | dbListFields.R |
| dbBind | PrestoResult | Not implemented | |
| dbColumnInfo | PrestoResult | Not implemented | |

## `dplyr` remote database backend

[`dplyr` generics][1]:

| Method | Primary class | Status | File |
| ------ | ------------- | ------ | ---- |
| db_desc | PrestoConnection | Implemented | dbplyr-db.R |
| db_data_type | PrestoConnection | Implemented | dbplyr-db.R |
| db_explain | PrestoConnection | Implemented | dbplyr-db.R |
| db_query_rows | PrestoConnection | Not implemented | |
| db_query_fields | PrestoConnection | Default | |
| db_save_query | PrestoConnection | Implemented | dbplyr-db.R |
| db_list_tables | PrestoConnection | Implemented | dbplyr-db.R |
| db_has_table | PrestoConnection | Implemented | dbplyr-db.R |
| db_write_table | PrestoConnection | Implemented | dbplyr-db.R |
| db_create_table | PrestoConnection | Not implemented | |
| db_insert_into | PrestoConnection | Not implemented | |
| db_drop_table | PrestoConnection | Not implemented | |
| db_begin | PrestoConnection | Not implemented | |
| db_rollback | PrestoConnection | Not implemented | |
| db_commit | PrestoConnection | Not implemented | |
| db_analyze | PrestoConnection | Not implemented | |

`dplyr` remote database source functions:

| Function | Default | Primary class | Status | File |
| -------- | ----------- | ------------- | ------ | ---- |
| src_presto | src_dbi | | Implemented | dbplyr-src.R |
| tbl | | PrestoConnection | Implemented | dbplyr-src.R |
| copy_to | | PrestoConnection | Implemented | dbplyr-src.R |
| tbl.src_presto | tbl.src_dbi | src_presto | Implemented | dbplyr-src.R |
| copy_to.src_presto | copy_to.src_sql | src_presto | Implemented | dbplyr-src.R |
| collect.tbl_presto | collect.tbl_sql | tbl_presto | Implemented | dbplyr-src.R |
| compute.tbl_presto | compute.tbl_sql | tbl_presto | Implemented | dbplyr-src.R |
| collapse.tbl_presto | collapse.tbl_sql | tbl_presto | Default | |

[`dbplyr` generics][2]:

| Method | Primary class | Status | File |
| ------ | ------------- | ------ | ---- |
| dbplyr_edition | PrestoConnection | Implemented | dbplyr-db.R |
| db_collect | PrestoConnection | Implemented | dbplyr-db.R |
| db_copy_to | PrestoConnection | Implemented | dbplyr-db.R |
| db_compute | PrestoConnection | Implemented | dbplyr-db.R |
| db_sql_render | PrestoConnection | Implemented | dbplyr-db.R |
| sql_query_fields | PrestoConnection | Implemented | dbplyr-sql.R |
| sql_escape_date | PrestoConnection | Implemented | dbplyr-sql.R |
| sql_escape_datetime | PrestoConnection | Implemented | dbplyr-sql.R |
| sql_translation | PrestoConnection | Implemented | dbplyr-sql.R |
| sql_query_save | PrestoConnection | Implemented | dbplyr-sql.R |

## Presto-specific functions

| Function | Primary class | Status | File |
| -------- | ------------- | ------ | ---- |
| presto_unnest | tbl_presto | Created | presto_unnest.R |

[1]: https://dplyr.tidyverse.org/reference/backend_dbplyr.html
[2]: https://dbplyr.tidyverse.org/reference/db-io.html
