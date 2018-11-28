import psycopg2
import pandas as pd
from sqlalchemy import create_engine

import pandas as pd
import numpy as np
import re

from create_staging_tables import create_staging_merged_final_df_STEP_FIVE


engine = create_engine('postgresql://localhost:5432/wa_leg_staging')
con = engine.connect()
legislator_df = pd.read_sql_query('select * from "legislator"',con=engine)
staging_vote_df = pd.read_sql_query('select * from "vote"',con=engine)
staging_bill_df = pd.read_sql_query('select * from "bill"',con=engine)

MERGED_final = create_staging_merged_final_df_STEP_FIVE(staging_vote_df, staging_bill_df, legislator_df)


def load_and_clean_party_minority_history_df():
    '''Load party minority history data from csv file and make agency and minority_party fields ints.'''

    minority_hist = pd.read_csv('party_minority_history.csv', sep="|")

    def change_agency_to_int(agency):
        if agency == 'House':
            return 0
        if agency == 'Senate':
            return 1

    def change_party_letter_to_int(party):
        if party == 'D':
            return 0
        if party == 'R':
            return 1
        if party == 'Even':
            return 2

    minority_hist['agency'] = minority_hist['agency'].apply(change_agency_to_int)
    minority_hist['minority_party'] = minority_hist['minority_party'].apply(change_party_letter_to_int)
    return minority_hist



def clean_merged_final(MERGED_final):

    MERGED_valid = MERGED_final[MERGED_final['primary_sponsor_id'].notnull()]
    clean_df = MERGED_valid.drop(['name', 'first_name', 'last_name', 'id',
                                  'agency', 'long_friendly_name'], axis=1)

    def change_agency_to_int(agency):
        if agency == 'House':
            return 0
        if agency == 'Senate':
            return 1

    def make_sec_sponsors_a_list(sponsors):
        if type(sponsors) == str:
            clean_sponsors = sponsors.strip('{}')
            id_list = clean_sponsors.split(',')
            return id_list
        else:
            return sponsors

    clean_df['sponsor_agency'] = clean_df['sponsor_agency'].apply(change_agency_to_int)
    clean_df['primary_sponsor_id'] = clean_df['primary_sponsor_id'].apply(int)
    clean_df['secondary_sponsors'] = clean_df['secondary_sponsors'].apply(make_sec_sponsors_a_list)
    clean_df['secondary_sponsors'].fillna('', inplace=True)
    clean_df['is_primary_sponsor'] = clean_df['voter_id'] == clean_df['primary_sponsor_id']

    minority_hist = load_and_clean_party_minority_history_df()

    def make_is_minority_party(row):
        subset_m_history = minority_hist[(minority_hist['biennium'] == row['biennium']) &
                                         (minority_hist['agency'] == row['voting_agency'])]
        return (subset_m_history['minority_party'] == row['party']).iloc[0]

    def make_is_secondary_sponsor(row):
        secondary_sponsors = row['secondary_sponsors']
        if len(secondary_sponsors) > 0:
            voter_id = str(row['voter_id'])
            return voter_id in secondary_sponsors
        else: return False

    def find_sponsor_party(row):
        ps_id = row['primary_sponsor_id']
        ps_agency = row['sponsor_agency']
        subset_leg = legislator_df[(legislator_df['id'] == int(ps_id)) &
                                   (legislator_df['agency'] == int(ps_agency))]
        sponsor_party = subset_leg.iloc[0, 4]
        return sponsor_party

#     def find_first_read_date(bill_text):
#         text_lst = bill_text.split('Read first time ')
#         read_first_time_date = text_lst[1][0:8]
#         print(text_lst[1][0:8])
#     clean_df['read_first_time_date'] = clean_df['bill_text'].apply(find_first_read_date)

    clean_df['is_minority_party'] = clean_df.apply(make_is_minority_party, axis=1)
    clean_df['is_secondary_sponsor'] = clean_df.apply(make_is_secondary_sponsor, axis=1)
    clean_df['sponsor_party'] = clean_df.apply(find_sponsor_party, axis=1)
    clean_df = clean_df.drop(['primary_sponsor_id', 'secondary_sponsors', 'htm_create_date', 'year'], axis=1)

    return clean_df

clean_df = clean_merged_final(MERGED_final)

clean_df.iloc[0:100000, :].to_sql('merged_final', con, if_exists='replace', index=True)
intervals = np.linspace(100000, 3100000, 31, dtype=int)
in_database = []

for interval in intervals:
    MERGED_final.iloc[interval:interval+100000, :].to_sql('merged_final', con, if_exists='append', index=True)
    in_database.append((interval, interval + 100000))
    print(in_database[-1])

con.close()
