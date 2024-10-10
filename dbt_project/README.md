# General Flow of Data
Preface: I loaded the csv files as seeds. But I would not do this for a production process where we receive files evey month.
## One staging table for each source `stg_sourceX`
This is where cleansing and standardization is performed. Example: standardize phone numbers, alias to standardized column names. These are incremental models. 
The date the file is received is added in these tables.
## Downstream prep table that combines each `stg_source` table: `fct_leads`
This table will contain all columns across all source staging tables. It utilizes the `union_relations` macro to manage
differing column coverage across staging tables. This is also an incremental model. We will load new leads into Salesforce based on the file_loaded_at column.
## Downstream analytics layer table that provides a history of each lead `fct_leads_hist`
This table builds off `fct_leads` and tracks the history of each lead. We also add in the salesforce loaded at date, based on a join to a theoretical salesforce source table that was generated
through the loading of the previous table into salesforce.

# ELT / ETL Notes

For monthly external files, I would utilize cloud storage like S3. CSVs can be sent here. Ideally, the file name would have the date in it. In order to account for changing
file schemas, we could create a python script to dynamically add columns to the staging table. Snowflake has some functionality to make this
a bit easier, namely utilizing external stages and tables, if we chose to use Snowflake. We could also use AWS Glue if we were on Redshift.

I would orchestrate this with Airflow, and store the airflow run date in each `stg_source` table.

# Trade-offs
- source_file fields in the staging tables would be dynamic and not hardcoded
- I would spend extra time parsing first and last names, and parsing address on source1. Zip is the most important thing to parse, 
because I would probably just use a publicly available zip code lookup dataset to extract the city and county information.
- Spend some extra time normalizing the dates to a consistent format
- Documentation would be more extensive
- The logic for fct_leads_hist is unrefined. Ideally we'd not populate a new row for the same lead. However, we should still be able to query this table and 
answer the question of "how many duplicate leads did we get from a file"





