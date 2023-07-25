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