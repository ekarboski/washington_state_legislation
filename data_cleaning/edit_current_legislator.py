import psycopg2
from sqlalchemy import create_engine

import pandas as pd
import numpy as np

engine = create_engine('postgresql://emilykarboski@localhost:5432/wa_leg_staging')
con = engine.connect()
current_legislator_df = pd.read_sql_query('select * from "current_legislator"',con=engine)

def change_agency_to_int(agency):
    """Change agency (strt) to int. House = 0, Senate = 1"""
    if agency == 'House':
        return 0
    if agency == 'Senate':
        return 1

# current_legislator_df['agency'] = current_legislator_df['agency'].apply(change_agency_to_int)
# current_legislator_df.rename(columns={'id': 'voter_id', 'agency': 'voting_agency'}, inplace=True)

def filter_out_duplicates_from_current_leg(row):
    """Some legislators have had many previous roles. Over the years they may have changed from a 
    representative to a senator, changed parties or districts. This function will filter out the past
    versions of legislators so that all that remains are the version that aligns with their current 
    agency, party and district.

    Args:
        row: row of dataframe
    
    Returns: boolean. True if the row has current info, False if row had past info.

    Example use: df.apply(filter_out_duplicates_from_current_leg, axis=1)
    """
    if row['last_name'] == 'Angel' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Bailey' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Billig' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Carlyle' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Chase' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Cody' and row['district'] == 11:
        return False
    if row['last_name'] == 'Conway' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Darneille' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Ericksen' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Fortunato' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Fortunato' and row['district'] == 47:
        return False
    if row['last_name'] == 'Frockt' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Hasegawa' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Hawkins' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Honeyford' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Hunt' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Keiser' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Kuderer' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Liias' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'McCoy' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Miloscia' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Miloscia' and row['party'] == 0:
        return False
    if row['last_name'] == 'Nelson' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == "O'Ban" and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Orcutt' and row['district'] == 18:
        return False
    if row['last_name'] == 'Padden' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Pedersen' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Rivers' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Rolfes' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Schoesler' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Sheldon' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Short' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Stonier':
        return False
    if row['last_name'] == 'Takko' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Van' and row['first_name'] == 'Kevin' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Walsh' and row['first_name'] == 'Maureen' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Warnick' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Wilson' and row['voting_agency'] == 0:
        return False
    if row['last_name'] == 'Zeiger' and row['voting_agency'] == 0:
        return False
    return True


current_legislator_df['current'] = current_legislator_df.apply(filter_out_duplicates_from_current_leg, axis=1)
current_legislator_df = current_legislator_df[current_legislator_df['current'] == True]

current_legislator_df.to_sql('current_legislator', con, if_exists='replace', index=False)