import pandas as pd
import numpy as np

import psycopg2
from sqlalchemy import create_engine


def load_label_data():
    engine = create_engine('postgresql://localhost:5432/wa_leg_label')
    label_df = pd.read_sql_query('select * from "label"',con=engine)
    return label_df


def select_one_bill_from_label_df(bill_num):
    label_df = load_label_data()
    selected_label_df = label_df[label_df['bill_num'] == bill_num]

    senate_selected_label_df = selected_label_df[selected_label_df['voting_agency'] == 1]
    sorted_senate = senate_selected_label_df.sort_values('last_name')
    sorted_senate = sorted_senate.reset_index().drop('index', axis=1)

    house_selected_label_df = selected_label_df[selected_label_df['voting_agency'] == 0]
    sorted_house = house_selected_label_df.sort_values('last_name')
    sorted_house = sorted_house.reset_index().drop('index', axis=1)

    return sorted_senate, sorted_house
