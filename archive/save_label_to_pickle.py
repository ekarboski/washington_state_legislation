import psycopg2 as pg2
import pandas as pd
import sqlalchemy
import pickle

engine = sqlalchemy.create_engine('postgresql://emilykarboski@localhost:5432/wa_leg_label')
label_df = pd.read_sql_query('select * from "label"',con=engine)

label_df.to_pickle('label_pickle.pkl')