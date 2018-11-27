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

# MERGED_final.iloc[0:10000, :].to_sql('merged_final', con, if_exists='replace', index=True)

# 1130000-1400000 skipped
# 1900000-1910000 skipped
# 1930000-1940000 skipped
# 2370000-2380000 skipped
# 2530000-2540000 skipped
intervals = np.linspace(2380000, 3170000, 80, dtype=int)
in_database = []

for interval in intervals:
    MERGED_final.iloc[interval:interval+10000, :].to_sql('merged_final', con, if_exists='append', index=True)
    in_database.append((interval, interval + 10000))
    print(in_database[-1])

con.close()