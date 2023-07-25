
# dbt sandbox for Dynamic tables

the intent here was to make a sandbox for testing dynamic tables that:
- felt responsive enought that I could count on there being more rows generated every minute
- didn't require me to learn and implement Kafka,
- pay for an streaming dataset in the marketplace

the approach is:

1. use snowpark to create a table with 10,000 timestamps over the next two hours
2. make a view that selects from above table, but filters to only include rows whose timestamps are now in the past (`WHERE DATETIME <= SYSDATE()`)
3. build a Dynamic Table on top of the view that could "catch" the new rows in the view result and append them to the table


## result

Dynamic Table does NOT refresh after it's creation.

## theories

perhaps it is because the underlying view object and definition are not changing, nor is the upstream table of timestamps?

## models


this project has three models

1. `my_fake_model.py`
2. `my_view.sql`
3. `my_dt.sql`

### `my_fake_model.py`

makes a table of 10,000 rows with two columns:

- `TIMESTAMP` timestamps b/w the time the model is run and two hours in the future
- `COLORS` randomly selected color names



<details>
<summary>python model</summary>

```py
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
```

</details>

### `my_view.sql`

```sql
{{
  config(
    materialized = 'view',
    )
}}
SELECT * FROM {{ ref('my_fake_data') }}
WHERE DATETIME <= SYSDATE()
```


### `my_dt.sql`

```sql
{{
  config(
    materialized = 'dynamic_table',
    target_lag = '1 minute',
    warehouse = 'DBT_TESTING'
    )
}}

SELECT * FROM {{ ref('my_view') }}
```