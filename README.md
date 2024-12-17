# trust_in_government_etl
## Creation of a Trust in Government database

This repo contains the Prefect deployment of a pipeline that creates a table storing Trust in Government survey statistics from the American National Elections Studies (ANES) and additional variables taken from the Bureau of Economic Analysis (BEA) and American Community Survey (ACS conducted by the US Census Bureau). This data is stored in an effort to support analysis of Trust in Government by creating a curated dataset that can be iterated, updated, and exposed for visualization of additional analysis.

## Pipeline Structure

The pipeline consists of three primary parts: retrieval, upload, and transformation.

### 1. Retrieval
First, we set up separate Tasks to retrieve data from the four source locations.

The first three tasks share a common structure of connecting to a site-specific API and retreiving data in a JSON structure. All steps use the `requests` library.

- `get_gdp_data`: Task to connect to the BEA API and retireve GDP by state (JSON).
- `get_rpp_data`: Task to connect to the BEA API and retireve Regional Price Parity index by state (JSON).
- `get_acs_data`: Task to connect to the ACS API and retrieve various state level statistics on employment, income, and population (JSON).

The final retrieval is the ANES survey data. There is currently no API for this data, but it is only updated once every four years post-US presidential election cycles. Due to these constraints we download the data in CSV format and store it in an Amazon S3 bucket. Steps include the use of `boto3` and `pandas` libraries. 

- `get_anes_data`: Task to connect to Amazon S3 and retrieve a specific CSV file saved with the 2020 ANES survey results.

### 2. Data Ingestion

The next step is uploading all three sources and four retrieved tables into a PostgreSQL database that is hosted on Heroku. This databse was set up prior to the pipeline and includes one existing table, `dim_geo`, that will be used to connect the tables we create in this step of the pipeline.

All four tasks in this section follow a similar structure. 
- First, we connect to the Postgres databse using `psycopg2`.
- Second, we create a table in the databse with the appropriate structure to handle each data source.
- Third, we insert the data from the first step to their corresponding tables we just created.

### 3. Data Transformation

The final step in our pipeline is to combine the four tables and transform them to include some new variables that will provde useful for analysis. This step includes three distinct Tasks: aggregate the ANES data, combine the data sources, and transform the variables into a final table. All three Tasks rely on the `psycopg2` library and the final transformation step uses the `SQLAlchemy` and `pandas` libraries heavily.

- `aggregate_anes_data`: The survey results are individual responses so we group them by the state the respondent was registered to vote in at the time of the survey (all pre-election results by the way) and aggreate two variables related to Trust in Government. The average response (on a 5 point scale, higher being less trust) and the percentage of people who said they trust the government at least "Most of the time". This is saved in a new table.
- `combine_data`: Next we join all of the tables to the dim_geo table which has a shared variable for all of the tables (state identifiers). This is saved in a new table.
- `transform_data`: In this step we create a `pandas`' dataframe from the `combined_data` table and create five new variables: `gdp_per_capita`, `employement_rate`, `gdp_at_rpp`, `gdp_per_capita_at_rpp`, and `median_household_income_at_rpp`. These are specific variabels of interest for addtional study/analysis.

## Final Database Structure

The final database structure, at the moment, is a single schema that includes all of the tables created in this process. My intention is to create new schema where only the `final_table` and `dim_geo` tables are in the "mart" section and all other tables are in staging, possibly moving the `combined_table` to be a View in the Flow for efficiency. But, frankly, the dataset is so small overall that storage and efficiency are not big problems.

The final table now includes:

- `geo_name`: The state name.
- `avg_trust_in_gov`: Average response to Trust in Government question for all respondents in the state.
- `perct_trust_in_gov`: Percent of respondents in the state who said they trust the government at least "Most of the time".
- `civ_labor_force_total`: Total number of eligible civilian labor force population in the state (excluding military).
- `civ_employed`: Total number of civilians that are employed in the steate.
- `median_household_income`: The median househoold income for the state.
- `total_population`: Total population for the state.
- `gdp_cl_unit`: The unit used for the `gdp_value` field.
- `gdp_unit_multi`: The multiplier used for the `gdp_value` field.
- `gdp_value`: The GDP for the state (i.e. in `gdp_cl_unit` units). 
- `rpp_cl_unit`: The unit used for the `rpp_value` field.
- `rpp_unit_multi`: The multiplier used for the `rpp_value` field.
- `rpp_value`: The Regional Price Parity index for the state (i.e. in `rpp_cl_unit` units).
- `anes_timeperiod`: The time period the ANES survey responses are from (i.e. 2020 and soon to be added 2024).
- `acs_timeperiod`: The time period the ACS data is from.
- `gdp_timeperiod`: The time period the GDP data is from.
- `rpp_timeperiod`: The time period the RPP data is from.

## Recurrence

The BEA data is updated most frequently at once per quarter so we set our CRON schedule to be done once every three months on the 28th data of March, June, September, and December. The ACS data is typically updated in September while the ANES data is expected to be published in February (preliminary) and June (final) so the schedule will update these relatively quickly after their release as well.

The ANES data will need to be udpated manually and the Flow along with it so there is some ongoing maintenance built into the pipeline. For the other data sources I set up a schedule of retries to ensure we capture the most recent data where we specify the YEAR of the data as the current year and if that fails we decrement by one year on the next retry if no update has been made in the current year.

Worth mentioning that 
## Data Management

I tried to implement some data management practices along the way as well. This includes:

- `dblast_updated` field includes to time stamp when the row was uploaded to the database. This can be used for debugging, etc.
- Duplicate prevention techniques included in the SQL to prevent duplicate rows from being added. This will need better handling in the next iteration.
- `CREATE IF NOT EXISTS` used to not duplicate tables but ensure the tables exist when uploading data.
- Scalability in mind by using dimensional tables (i.e. dim_geo) so we can expand the data captured to counties in a future iteration.
- Data time periods includes on all tables to facilitate time period comparison or time series analysis as this process continues (i.e. tables will be udpated with data from 2023, 2024, 2025, etc.).


## Security

Needs some updating but I used Prefect Secrets to store API keys and other passwords. Pretty nice feature actually.

## Error handling and communication

## Improvements and Next Steps
- Error handling (duplicates, etc.)
- Make combined_table a View.
- Include county level data (may not be useful with survey results but needs to be varified).
- Variable expansion.

