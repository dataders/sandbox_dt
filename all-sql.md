# repro for sasha

## the sql

### `my_fake_data`

#### make stored procedure

<details><summary>Python SPROC def</summary>

```sql
WITH my_fake_data__dbt_sp AS PROCEDURE ()

RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('faker', 'pandas', 'snowflake-snowpark-python')

HANDLER = 'main'
EXECUTE AS CALLER
AS
$$

import sys
sys._xoptions['snowflake_partner_attribution'].append("dbtLabs_dbtPython")


  
    
import pandas as pd
from faker import Faker
import pytz

def get_future_timestamp(fkr_inst, interval, timezone):
    return fkr_inst.future_datetime(
        end_date=interval,
        tzinfo=timezone
        )

def model(dbt, session):
    dbt.config(
        materialized = "table",
        packages = ["faker", "pandas"]
    )
    fake = Faker()
    tz_pdt = pytz.timezone('America/Los_Angeles')


    df = (
        pd.DataFrame(
            [get_future_timestamp(fake, "+2h", tz_pdt) for _ in range(10000)],
            columns=['DATETIME']
        )
        .assign(
            COLOR= lambda df_: [fake.color_name() for i in range(len(df_))]
        )
        .sort_values('DATETIME')
        .reset_index(drop=True)
    )

    return df


# This part is user provided model code
# you will need to copy the next section to run the code
# COMMAND ----------
# this part is dbt logic for get ref work, do not modify

def ref(*args, **kwargs):
    refs = {}
    key = '.'.join(args)
    version = kwargs.get("v") or kwargs.get("version")
    if version:
        key += f".v{version}"
    dbt_load_df_function = kwargs.get("dbt_load_df_function")
    return dbt_load_df_function(refs[key])


def source(*args, dbt_load_df_function):
    sources = {}
    key = '.'.join(args)
    return dbt_load_df_function(sources[key])


config_dict = {}


class config:
    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def get(key, default=None):
        return config_dict.get(key, default)

class this:
    """dbt.this() or dbt.this.identifier"""
    database = "TEST_DB"
    schema = "dbt_ajs"
    identifier = "my_fake_data"
    
    def __repr__(self):
        return 'TEST_DB.dbt_ajs.my_fake_data'


class dbtObj:
    def __init__(self, load_df_function) -> None:
        self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
        self.ref = lambda *args, **kwargs: ref(*args, **kwargs, dbt_load_df_function=load_df_function)
        self.config = config
        self.this = this()
        self.is_incremental = False

# COMMAND ----------

# To run this in snowsight, you need to select entry point to be main
# And you may have to modify the return type to text to get the result back
# def main(session):
#     dbt = dbtObj(session.table)
#     df = model(dbt, session)
#     return df.collect()

# to run this in local notebook, you need to create a session following examples https://github.com/Snowflake-Labs/sfguide-getting-started-snowpark-python
# then you can do the following to run model
# dbt = dbtObj(session.table)
# df = model(dbt, session)


def materialize(session, df, target_relation):
    # make sure pandas exists
    import importlib.util
    package_name = 'pandas'
    if importlib.util.find_spec(package_name):
        import pandas
        if isinstance(df, pandas.core.frame.DataFrame):
          session.use_database(target_relation.database)
          session.use_schema(target_relation.schema)
          # session.write_pandas does not have overwrite function
          df = session.createDataFrame(df)
    
    df.write.mode("overwrite").save_as_table('TEST_DB.dbt_ajs.my_fake_data', create_temp_table=False)

def main(session):
    dbt = dbtObj(session.table)
    df = model(dbt, session)
    materialize(session, df, dbt.this)
    return "OK"

$$
CALL my_fake_data__dbt_sp();
```

</details>

#### stuff `.write_pandas()` does

```sql
CREATE TEMP STAGE /* Python:snowflake.connector.pandas_tools.write_pandas() */
"TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_STAGE_KSDSJYXGPH"
FILE_FORMAT=(TYPE=PARQUET COMPRESSION=gzip BINARY_AS_TEXT=FALSE);

CREATE TEMP FILE FORMAT "TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_FILE_FORMAT_QXKLUQIYMM"
/* Python:snowflake.connector.pandas_tools.write_pandas() */
TYPE=PARQUET COMPRESSION=auto;

SELECT COLUMN_NAME, TYPE
FROM table(infer_schema(
    location=>'@"TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_STAGE_KSDSJYXGPH"',
    file_format=>'"TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_FILE_FORMAT_QXKLUQIYMM"'));

CREATE TEMPORARY TABLE IF NOT EXISTS "TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_TABLE_CKY6M1LI1F" (
    "DATETIME" TIMESTAMP_NTZ, "COLOR" TEXT) /* Python:snowflake.connector.pandas_tools.write_pandas() */

COPY INTO "TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_TABLE_CKY6M1LI1F"
/* Python:snowflake.connector.pandas_tools.write_pandas() */ (
    "DATETIME","COLOR")
FROM (
    SELECT
        $1:"DATETIME"::TIMESTAMP_NTZ,
        $1:"COLOR"::TEXT 
    FROM @"TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_STAGE_KSDSJYXGPH"
    )
FILE_FORMAT=(TYPE=PARQUET COMPRESSION=auto BINARY_AS_TEXT=FALSE)
PURGE=TRUE
ON_ERROR=abort_statement;
```

#### actual table creation

```sql
CREATE OR  REPLACE TABLE TEST_DB.dbt_ajs.my_fake_data AS
    SELECT * FROM ( SELECT * FROM ("TEST_DB"."DBT_AJS"."SNOWPARK_TEMP_TABLE_CKY6M1LI1F"))
```

### `my_view`

```sql
create or replace view TEST_DB.dbt_ajs.my_view as (

    SELECT * FROM TEST_DB.dbt_ajs.my_fake_data
    WHERE DATETIME <= SYSDATE()
);
```

### `my_dt`

```sql
drop dynamic table if exists "TEST_DB"."DBT_AJS"."MY_DT";

create or replace dynamic table TEST_DB.dbt_ajs.my_dt
    target_lag = '1 minute'
    warehouse = DBT_TESTING
    as (
        SELECT * FROM TEST_DB.dbt_ajs.my_view
);

alter dynamic table TEST_DB.dbt_ajs.my_dt refresh
```

###