import psycopg2
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from create_staging_tables import create_staging_merged_final_df_STEP_SIX

engine = create_engine('postgresql://localhost:5432/wa_leg_staging')
con = engine.connect()

merged_initial_df = pd.read_sql_query('select * from "merged_initial"',con=engine)
bill_text_df =      pd.read_sql_query('select * from "bill_text"',con=engine)
legislator_df =     pd.read_sql_query('select * from "legislator"',con=engine)

MERGED_final = create_staging_merged_final_df_STEP_SIX(merged_initial_df, bill_text_df, legislator_df)

intervals = np.linspace(950000, 3170000, 223, dtype=int)

for interval in intervals:
    MERGED_final.iloc[interval:interval+10000, :].to_sql('merged_final', con, if_exists='append', index=False)
    print((interval, interval + 10000))

con.close()