from datetime import datetime
import pandas as pd
import requests
import psycopg2
import boto3
from sqlalchemy import create_engine
from io import StringIO
from prefect import flow, task
from prefect.blocks.system import Secret
#from prefect.server.schemas.schedules import CronSchedule
#from prefect_github.repository import GitHubRepository


@task(retries=5, retry_delay_seconds=10)
def get_gdp_data(
    key = Secret.load("bea-api-key").get(),
    dataset = 'Regional',
    table = 'SARPP',
    geofips = 'STATE',
    resultformat = 'json',
):
    from prefect.context import get_run_context
    context = get_run_context()
    retry_count = context.task_run.run_count

    # Compute the year based on retry attempt
    current_year = datetime.now().year
    year = current_year - retry_count

    api_url = f'https://apps.bea.gov/api/data/?UserID={key}&method=GetData&datasetname={dataset}&TableName={table}&LineCode=1&Year={year}&GeoFips={geofips}&ResultFormat={resultformat}'
    response = requests.get(api_url)
    gdp_raw = response.json()
    gdp_clean = gdp_raw['BEAAPI']['Results']['Data']
    return gdp_clean

@task(retries=5, retry_delay_seconds=10)
def get_rpp_data(
    key = Secret.load("bea-api-key").get(),
    dataset = 'Regional',
    table = 'SARPP',
    geofips = 'STATE',
    resultformat = 'json'
    ):
    from prefect.context import get_run_context
    context = get_run_context()
    retry_count = context.task_run.run_count

    # Compute the year based on retry attempt
    current_year = datetime.now().year
    year = current_year - retry_count

    api_url = f'https://apps.bea.gov/api/data/?UserID={key}&method=GetData&datasetname={dataset}&TableName={table}&LineCode=1&Year={year}&GeoFips={geofips}&ResultFormat={resultformat}'
    response = requests.get(api_url)
    rpp_raw = response.json()
    rpp_clean = rpp_raw['BEAAPI']['Results']['Data']
    return rpp_clean

@task(retries=5, retry_delay_seconds=10)
def get_acs_data(
    dataset='1',
    table='profile',
    variables = "NAME,DP03_0003E,DP03_0006E,DP03_0004E,DP03_0062E,DP05_0001E,DP03_0001E",
    geo="state",
    key=Secret.load("acs-api-key") .get()
    ):
    from prefect.context import get_run_context
    context = get_run_context()
    retry_count = context.task_run.run_count

    # Compute the year based on retry attempt
    current_year = datetime.now().year
    year = current_year - retry_count
    acs_year = year

    api_url = f'https://api.census.gov/data/{year}/acs/acs{dataset}/{table}?get={variables}&for={geo}&key={key}'
    response = requests.get(api_url)
    acs_raw = response.json()
    acs_clean = acs_raw[1:]
    return acs_clean, acs_year

@task
def get_anes_data():
    key = Secret.load("aws-s3-key").get()
    secret_key = Secret.load("aws-s3-secret-key").get()
    client = boto3.client("s3",
                        aws_access_key_id=key,
                        aws_secret_access_key=secret_key,
                        region_name = "us-east-2")

    anes_object = client.get_object(Bucket = "dacss690a-bucket",
                                        Key = "anes_timeseries_2020_csv_20220210.csv")

    anes_raw = anes_object["Body"].read().decode("utf-8", errors="replace")
    anes_full = pd.read_csv(StringIO(anes_raw))
    anes_clean = anes_full.loc[:, ['V200005','V200006','V201650','V201233','V201575','V201016','V201103','V201200','V201206','V201207','V201231x','V201232','V201363','V201435','V201507x','V201508','V201509','V201511x','V201516','V201534x','V201544','V201549x','V201567','V201600','V201601','V201606','V201617x','V201620','V201623','V201628','V201005','V201114','V201115','V201116','V201117','V201118','V201119','V201120','V201121','V201122','V201123','V201126x','V201129x','V201225x','V201234','V201235','V201236','V201237','V201238','V201246','V201252','V201255','V201258','V201262','V201302x','V201305x','V201308x','V201311x','V201314x','V201317x','V201320x','V201323x','V201324','V201327x','V201330x','V201333x','V201334','V201342x','V201345x','V201352','V201353','V201359x','V201366','V201367','V201368','V201369','V201372x','V201375x','V201376','V201377','V201382x','V201400x','V201401','V201428','V201433','V201502','V201503','V201602','V201605x','V201651', 'V201014b']]
    return anes_clean

@task
def upload_gdp_data(gdp_clean):
    current_timestamp = datetime.now()
    user = Secret.load("heroku-user").get()
    password = Secret.load("heroku-password").get()
    current_quarter = (datetime.now().month - 1) // 3 + 1
    conn = psycopg2.connect(
        host="c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
        database="dc764nhuir4ilo",
        user=user,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS bea_regional_real_gdp (
                code varchar,
                GeoFips integer PRIMARY KEY,
                GeoName varchar,
                TimePeriod integer,
                Quarter integer,
                cl_unit varchar,
                unit_mult numeric,
                DataValue numeric,
                dblast_update TIMESTAMPTZ
                )
                """
                )

    def insert_gdp(gdp_clean):
        for gdp in gdp_clean:
            cursor.execute("""
            INSERT INTO bea_regional_real_gdp (code, GeoFips, GeoName, TimePeriod, Quarter, cl_unit, unit_mult, DataValue, dblast_update)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, 
            (gdp['Code'], gdp['GeoFips'], gdp['GeoName'], gdp['TimePeriod'],
            current_quarter,gdp['CL_UNIT'], gdp['UNIT_MULT'], float(gdp['DataValue']), current_timestamp))
        conn.commit()

    insert_gdp(gdp_clean)
    conn.close()

@task
def upload_rpp_data(rpp_clean):
    current_timestamp = datetime.now()
    user = Secret.load("heroku-user").get()
    password = Secret.load("heroku-password").get()
    current_quarter = (datetime.now().month - 1) // 3 + 1

    conn = psycopg2.connect(
        host="c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
        database="dc764nhuir4ilo",
        user=user,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS bea_regional_rpp (
                code varchar,
                GeoFips integer PRIMARY KEY,
                GeoName varchar,
                TimePeriod integer,
                Quarter integer,
                cl_unit varchar,
                unit_mult numeric,
                DataValue numeric,
                dblast_update TIMESTAMPTZ
                )
                """
                )

    def insert_rpp(rpp_clean):
        for rpp in rpp_clean:
            cursor.execute("""
            INSERT INTO bea_regional_rpp (code, GeoFips, GeoName, TimePeriod, Quarter, cl_unit, unit_mult, DataValue, dblast_update)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, 
            (rpp['Code'], rpp['GeoFips'], rpp['GeoName'], rpp['TimePeriod'],
            current_quarter, rpp['CL_UNIT'], rpp['UNIT_MULT'], float(rpp['DataValue']), current_timestamp))
        conn.commit()

    insert_rpp(rpp_clean)
    conn.close()

@task
def upload_acs_data(acs_clean, acs_year):
    current_timestamp = datetime.now()
    user = Secret.load("heroku-user").get()
    password = Secret.load("heroku-password").get()
    conn = psycopg2.connect(
        host="c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
        database="dc764nhuir4ilo",
        user=user,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute("""
                CREATE TABLE IF NOT EXISTS acs_state_pop_stats (
                GeoName varchar,
                civ_labor_force_total integer,
                armed_forces integer,
                civ_employed integer,
                median_household_income integer,
                total_population integer,
                over16_population integer,
                geo_id integer,   
                TimePeriod integer,
                dblast_update TIMESTAMPTZ,
                PRIMARY KEY (geo_id, TimePeriod)
                )
                """
                )

    def insert_acs(acs_clean,acs_year):
        for acs in acs_clean:
            cursor.execute("""
            INSERT INTO acs_state_pop_stats (GeoName, civ_labor_force_total, armed_forces, civ_employed, median_household_income, total_population, over16_population, geo_id, TimePeriod, dblast_update)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (geo_id, TimePeriod) DO NOTHING;                           
            """, 
            (
            acs[0], int(acs[1]), int(acs[2]), int(acs[3]), int(acs[4]), int(acs[5]), int(acs[6]), int(acs[7]), int(acs_year), current_timestamp)
            )
        conn.commit()

    insert_acs(acs_clean,acs_year)
    conn.close()

@task
def upload_anes_data(anes_clean):
    engine = create_engine(f"postgresql+psycopg2://u9f2aus8s91ghq:p9947e452112ed87f0b60bfa3ae74596015aabbbf775efc652ea14a6a125ddd93@c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/dc764nhuir4ilo")
    table_name = "anes2020_raw_survey"
    anes_clean.to_sql(table_name, engine, if_exists="replace", index=False)

@task
def aggregate_anes_data():
    user = Secret.load("heroku-user").get()
    password = Secret.load("heroku-password").get()
    conn = psycopg2.connect(
        host="c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
        database="dc764nhuir4ilo",
        user=user,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute("""
                DROP TABLE IF EXISTS anes_state_agg;

                WITH filtered_anes AS (
                SELECT "V201014b" AS res_state,
                        "V200005" AS eligible_concern,
                        "V201650" AS eligible_surv_seriousness,
                        "V201233" AS trust_in_gov
                        
                FROM anes2020_raw_survey
                WHERE "V200005" = 0
                AND "V201650" IN (-9, -5, 4, 5)
                AND "V201233" IN (1,2,3,4,5)
                ),

                aggregate_anes AS (
                    SELECT res_state,
                            ROUND(CAST(AVG(trust_in_gov) AS numeric),2) AS avg_trust_in_gov,
                            ROUND(
                                CAST(COUNT(CASE WHEN trust_in_gov IN (1,2) THEN 1 END) AS numeric) / 
                                CAST(COUNT(CASE WHEN trust_in_gov IN (1,2,3,4,5) THEN 1 END) AS numeric),2) AS perct_trust_in_gov,
                            2020 AS timeperiod	
                    FROM filtered_anes
                    GROUP BY res_state
                )

                SELECT *
                INTO anes_state_agg
                FROM aggregate_anes
                """
                )
    conn.commit()
    conn.close()
                       
@task
def combine_data():
    user = Secret.load("heroku-user").get()
    password = Secret.load("heroku-password").get()
    conn = psycopg2.connect(
        host="c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
        database="dc764nhuir4ilo",
        user=user,
        password=password
    )
    cursor = conn.cursor()

    cursor.execute("""
                DROP TABLE IF EXISTS combined_table;

                CREATE TABLE combined_table AS
                SELECT geo.*,
                        anes.avg_trust_in_gov,
                        anes.perct_trust_in_gov,
                        acs.civ_labor_force_total,
                        acs.armed_forces,
                        acs.civ_employed,
                        acs.median_household_income,
                        acs.total_population,
                        acs.over16_population,
                        gdp.cl_unit AS gdp_cl_unit,
                        gdp.unit_mult AS gdp_unit_multi,
                        gdp.datavalue AS gdp_value,
                        rpp.cl_unit AS rpp_cl_unit,
                        rpp.unit_mult AS rpp_unit_multi,
                        rpp.datavalue AS rpp_value,
                        anes.timeperiod AS anes_timeperiod,
                        acs.timeperiod AS acs_timeperiod,
                        gdp.timeperiod AS gdp_timeperiod,
                        rpp.timeperiod AS rpp_timeperiod

                FROM dim_geo geo
                LEFT JOIN anes_state_agg anes ON anes.res_state = geo.geo_id
                LEFT JOIN acs_state_pop_stats acs ON acs.geo_id = geo.geo_id
                LEFT JOIN bea_regional_real_gdp gdp ON gdp.geofips = geo.geo_fips
                LEFT JOIN bea_regional_rpp rpp ON rpp.geofips = geo.geo_fips
            """
    )
    conn.commit()
    conn.close()

@task
def transform_data():
    user = Secret.load("heroku-user").get()
    password = Secret.load("heroku-password").get()
    conn = psycopg2.connect(
        host="c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com",
        database="dc764nhuir4ilo",
        user=user,
        password=password
    )
    cursor = conn.cursor()

    query = "SELECT * FROM combined_table;"
    df = pd.read_sql_query(query, conn)
    df = df.fillna("NA")

    df['gdp_per_capita'] = (df['gdp_value']*1000000) / df['total_population']
    df['gdp_per_capita'] = df['gdp_per_capita'].astype(int)

    df['employment_rate'] = (df['civ_employed'] / df['civ_labor_force_total']) * 100
    df['employment_rate'] = df['employment_rate'].round(2)

    df['gdp_at_rpp'] = (df['gdp_value'] * df['rpp_value']) / 100
    df['gdp_at_rpp'] = df['gdp_at_rpp'].astype(int)

    df['gdp_per_capita_at_rpp'] = (df['gdp_per_capita'] * df['rpp_value']) / 100
    df['gdp_per_capita_at_rpp'] = df['gdp_per_capita_at_rpp'].astype(int)

    df['median_household_income_at_rpp'] = (df['median_household_income'] * df['rpp_value']) / 100
    df['median_household_income_at_rpp'] = df['median_household_income_at_rpp'].astype(int)

    engine = create_engine(f"postgresql+psycopg2://u9f2aus8s91ghq:p9947e452112ed87f0b60bfa3ae74596015aabbbf775efc652ea14a6a125ddd93@c3nv2ev86aje4j.cluster-czrs8kj4isg7.us-east-1.rds.amazonaws.com:5432/dc764nhuir4ilo")
    table_name = "final_table"
    df.to_sql(table_name, engine, if_exists="replace", index=False)

@flow
def trust_in_government_pipeline():
    # Steps 1-4 can be run in parallel
    # Step 1: Load gdp data
    gdp_clean = get_gdp_data()

    # Step 2: Load rpp data
    rpp_clean = get_rpp_data()

    # Step 3: Load acs data
    acs_clean, acs_year = get_acs_data()

    # Step 4: Load anes data
    anes_clean = get_anes_data()

    # Steps 5-8 can be run in parallel
    # Step 5: Insert gdp data
    upload_gdp_data(gdp_clean)

    # Step 6: Insert rpp data
    upload_rpp_data(rpp_clean)

    # Step 7: Insert acs data
    upload_acs_data(acs_clean, acs_year)

    # Step 8: Insert anes data
    upload_anes_data(anes_clean)

    # Steps 9-11 must be run in succession
    # Step 9: Aggregate anes data
    aggregate_anes_data()

    # Step 10: Combined tables to single table
    combine_data()

    # Step 11: Create final table
    transform_data()

# Deploy the flow
#github_repository_block = GitHubRepository.load("trust-in-gov-github")

if __name__ == "__main__":
    flow.from_source(
        source = "https://github.com/tbartelloni/trust_in_government_etl.git",
        entrypoint = "trust_in_gov_pipeline.py:trust_in_government_pipeline"
    ).deploy(
        name="gov-trust-deployment",
        work_pool_name="MyWorkPool",
        cron="* * 28 3,6,9,12 *"
    )
    print("Good news everyone!")
