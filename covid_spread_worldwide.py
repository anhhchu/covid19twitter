#!/usr/bin/python
"""
Create a new aggregation for covid 19 spread worldwide based on bigquery-public-data.covid19_ecdc dataset 
"""

from google.cloud import bigquery
from google.cloud.bigquery import *
from google.cloud.bigquery.table import *

# Create a new Google BigQuery client using Google Cloud Platform project
# defaults.
bq = bigquery.Client()

# Create a new BigQuery dataset.
dataset_ref = bq.dataset("covid_spread_worldwide")
dataset = bigquery.Dataset(dataset_ref)
dataset = bq.create_dataset(dataset)

# In the new BigQuery dataset, create a new table.
table_ref = dataset.table('monthly')
# The table needs a schema before it can be created and accept data.
# We create an ordered list of the columns using SchemaField objects.
schema = []
schema.append(schema.SchemaField("countries_and_territories", "string"))
schema.append(schema.SchemaField("year", "integer"))
schema.append(schema.SchemaField("month", "integer"))
schema.append(schema.SchemaField("total_cases", "integer"))
schema.append(schema.SchemaField("total_deaths", "integer"))

# We assign the schema to the table and create the table in BigQuery.
table = bigquery.Table(table_ref, schema=schema)
table = bq.create_table(table)

# Next, we issue a query in StandardSQL.
# The query selects the fields of interest.
query = """
SELECT distinct countries_and_territories, year, month, sum(daily_confirmed_cases) as total_cases, sum(daily_deaths) as total_deaths
 FROM `bigquery-public-data.covid19_ecdc.covid_19_geographic_distribution_worldwide`
 group by countries_and_territories, year, month
"""

# We create a query job to save the data to a new table.
job_config = bigquery.QueryJobConfig()
job_config.write_disposition = 'WRITE_TRUNCATE'
job_config.destination = table_ref

# 'Submit the query.
query_job = bq.query(query, job_config=job_config)
