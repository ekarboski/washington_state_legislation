import pandas as pd
import numpy as np

import psycopg2
from sqlalchemy import create_engine


def load_label_data():
    engine = create_engine('postgresql://localhost:5432/wa_leg_label')
    label_df = pd.read_sql_query('select * from "label"',con=engine)
    return label_df

label_df = load_label_data()

def change_party_to_letter(party):
    if party == 0:
        return 'D'
    if party == 1:
        return 'R'


def select_one_bill_from_label_df(bill_id):
    '''Select bill from label_df that matches the inputted bill_num and return pandas dataframes of 
    the senate and the house, as well as the rep_score and dem_score for the bill.'''
    selected_label_df = label_df[label_df['bill_id'] == bill_id]
    selected_label_df['party'] =  selected_label_df['party'].apply(change_party_to_letter)

    senate_selected_label_df = selected_label_df[selected_label_df['voting_agency'] == 1]
    sorted_senate = senate_selected_label_df.sort_values('predicted_vote')
    sorted_senate = sorted_senate.reset_index().drop('index', axis=1)

    house_selected_label_df = selected_label_df[selected_label_df['voting_agency'] == 0]
    sorted_house = house_selected_label_df.sort_values('predicted_vote')
    sorted_house = sorted_house.reset_index().drop('index', axis=1)

    rep_score = sorted_senate.loc[0, 'rep_score']
    dem_score = 1 - rep_score
    url = sorted_senate.loc[0, 'htm_url']
    url = url.replace(' ', '%20')

    return sorted_senate, sorted_house, rep_score, dem_score, url

def get_bill_text(bill_id):
    engine = create_engine('postgresql://localhost:5432/wa_leg_staging')
    current_bill_text_df = pd.read_sql_query('select * from "current_bill_text"',con=engine)
    bill = current_bill_text_df[current_bill_text_df['bill_id'] == 'ESHB 2299']
    return bill.iloc[0, -1]

def calculate_lean_percentages(table):
    leaning_yea = len(table[table['predicted_vote'] > 0.65]) / len(table)
    leaning_nay = len(table[table['predicted_vote'] < 0.35]) / len(table)
    undecided = 1 - leaning_yea - leaning_nay
    return leaning_yea, leaning_nay, undecided