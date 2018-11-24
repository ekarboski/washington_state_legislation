import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin



def create_staging_legislator_df():
    """Merge votes_df, committee_df and missing_legislator_info_df and clean data to create legislator_df
    
    Input
    votes_df: pandas dataframe loaded from wa_leg_raw database, vote_api table
    committee_member_df: pandas dataframe loaded from wa_leg_raw database, committee_member_api table
    missing_legislator_info_df: pandas dataframe loaded missing_legislators.csv
    """

    engine = create_engine('postgresql://localhost:5432/wa_leg_raw')
    vote_df = pd.read_sql_query('select * from "vote_api"',con=engine)
    committee_member_df = pd.read_sql_query('select * from "committee_member_api"',con=engine)
    
    leg_info_from_vote_df = vote_df.loc[:, ['biennium', 'voter_id', 'voter_name', 'voting_agency']]
    leg_info_from_vote_df.drop_duplicates(keep='first', inplace=True)
    
    leg_info_from_cm_df = committee_member_df.loc[:, ['district', 'id', 'name', 'first_name', 'party']]
    leg_info_from_cm_df = leg_info_from_cm_df.rename(index=str, columns={"id": "voter_id"})
    leg_info_from_cm_df['last_name'] = leg_info_from_cm_df['name'].apply(lambda x: x.split()[1])
    leg_info_from_cm_df.drop_duplicates(keep='first', inplace=True)

    legislator_df = leg_info_from_vote_df.merge(leg_info_from_cm_df, how='outer', on='voter_id')
    legislator_df.drop_duplicates(keep="first", inplace=True)
    
    missing_leg_info_df = pd.read_csv('missing_legislators.csv', sep='|')
    missing_leg_info_df['district'] = missing_leg_info_df['district'].apply(str)
    missing_leg_info_df['voter_id'] = missing_leg_info_df['voter_id'].apply(str)
    
    legislator_df = legislator_df.merge(missing_leg_info_df, how='outer', on='voter_id', suffixes=('', '_missing'))
    
    def replace_missing_first_name(x):
        if type(x['first_name']) == float:
            return x['first_name_missing']
        else:
            return x['first_name']
        
    def replace_missing_last_name(x):
        if type(x['last_name']) == float:
            return x['last_name_missing']
        else:
            return x['last_name']
        
    def replace_missing_party(x):
        if type(x['party']) == float:
            if x['party_missing'] == 'R':
                return 'Republican'
            if x['party_missing'] == 'D':
                return 'Democrat'
        else:
            return x['party']
        
    def replace_missing_district(x):
        if type(x['district']) == float:
            return x['district_missing']
        else:
            return x['district']
        
    legislator_df['first_name'] = legislator_df.apply(lambda x : replace_missing_first_name(x),axis=1)
    legislator_df['party']      = legislator_df.apply(lambda x : replace_missing_party(x),     axis=1)
    legislator_df['district']   = legislator_df.apply(lambda x : replace_missing_district(x),  axis=1)
    legislator_df['last_name']  = legislator_df.apply(lambda x : replace_missing_last_name(x), axis=1)
    
    legislator_df = legislator_df.drop(['voter_name', 'name', 'district_missing', 'party_missing', 'first_name_missing', 'last_name_missing', 'biennium'], axis=1)
    legislator_df.drop_duplicates(keep='first', inplace=True)
    legislator_df.columns = ['id', 'agency', 'district', 'first_name', 'party', 'last_name']
    legislator_df['id'] = legislator_df['id'].apply(int)
    legislator_df['district'] = legislator_df['district'].apply(int)
    
    return legislator_df





class CreateStagingLegislatorDataframe(BaseEstimator, TransformerMixin):
    """Merge votes_df, committee_df and missing_legislator_df and clean data to create legislator_df"""
    
    def __init__(self, vote_df, committee_member_df, missing_leg_info_df):
        """Input
           votes_df: pandas dataframe loaded from wa_leg_raw database, vote_api table
           committee_member_df: pandas dataframe loaded from wa_leg_raw database, committee_member_api table
           missing_legislator_df: pandas dataframe loaded missing_legislators.csv"""
        self.vote_df = vote_df
        self.committee_member_df = committee_member_df
        self.missing_leg_info_df = missing_leg_info_df
    
    def fit(self, X=None, y=None):
        return self

    def transform(self, X=None):
        leg_info_from_vote_df = self.vote_df.loc[:, ['biennium', 'voter_id', 'voter_name', 'voting_agency']]
        leg_info_from_vote_df.drop_duplicates(keep='first', inplace=True)

        leg_info_from_cm_df = self.committee_member_df.loc[:, ['district', 'id', 'name', 'first_name', 'party']]
        leg_info_from_cm_df = leg_info_from_cm_df.rename(index=str, columns={"id": "voter_id"})
        leg_info_from_cm_df['last_name'] = leg_info_from_cm_df['name'].apply(lambda x: x.split()[1])
        leg_info_from_cm_df.drop_duplicates(keep='first', inplace=True)

        legislator_df = leg_info_from_vote_df.merge(leg_info_from_cm_df, how='outer', on='voter_id')
        legislator_df.drop_duplicates(keep="first", inplace=True)

        self.missing_leg_info_df['district'] = self.missing_leg_info_df['district'].apply(str)
        self.missing_leg_info_df['voter_id'] = self.missing_leg_info_df['voter_id'].apply(str)

        legislator_df = legislator_df.merge(self.missing_leg_info_df, how='outer', on='voter_id', suffixes=('', '_missing'))

        legislator_df['first_name'] = legislator_df.apply(lambda x : replace_missing_first_name(x),axis=1)
        legislator_df['party']      = legislator_df.apply(lambda x : replace_missing_party(x),     axis=1)
        legislator_df['district']   = legislator_df.apply(lambda x : replace_missing_district(x),  axis=1)
        legislator_df['last_name']  = legislator_df.apply(lambda x : replace_missing_last_name(x), axis=1)

        legislator_df = legislator_df.drop(['voter_name', 'name', 'district_missing', 'party_missing', 'first_name_missing', 'last_name_missing', 'biennium'], axis=1)
        legislator_df.drop_duplicates(keep='first', inplace=True)
        legislator_df.columns = ['id', 'agency', 'district', 'first_name', 'party', 'last_name']
        legislator_df['id'] = legislator_df['id'].apply(int)
        legislator_df['district'] = legislator_df['district'].apply(int)

        return legislator_df
    
    
    @staticmethod
    def replace_missing_first_name(x):
        if type(x['first_name']) == float:
            return x['first_name_missing']
        else:
            return x['first_name']
        
        
    @staticmethod
    def replace_missing_last_name(x):
        if type(x['last_name']) == float:
            return x['last_name_missing']
        else:
            return x['last_name']
        
        
    @staticmethod
    def replace_missing_party(x):
        if type(x['party']) == float:
            if x['party_missing'] == 'R':
                return 'Republican'
            if x['party_missing'] == 'D':
                return 'Democrat'
        else:
            return x['party']
        
        
    @staticmethod
    def replace_missing_district(x):
        if type(x['district']) == float:
            return x['district_missing']
        else:
            return x['district']
        
        
        
def create_staging_vote_df():
    '''Create statging_vote_df from raw_vote_df'''

    engine = create_engine('postgresql://localhost:5432/wa_leg_raw')
    con = engine.connect()
    vote_df = pd.read_sql_query('select * from "vote_api"',con=engine)
    vote_df['bill_unique'] = vote_df['biennium'] + ' ' + vote_df['bill_id']


    def change_biennium_to_year(biennium):
        return int(biennium[0:4])
    
    def change_vote_to_int(vote):
        if vote == 'Nay':
            return 0
        if vote == 'Yea':
            return 1
        if vote == 'Excused':
            return 2
        if vote == 'Absent':
            return 3

    def change_agency_to_int(agency):
        if agency == 'House':
            return 0
        if agency == 'Senate':
            return 1

        
    vote_df['year'] = vote_df['biennium'].apply(change_biennium_to_year)
    vote_df = vote_df.drop(['bill_id', 'biennium', 'motion'], axis=1)
    vote_df['sequence_number'] = vote_df['sequence_number'].apply(int)
    vote_df['voter_id'] = vote_df['voter_id'].apply(int)
    vote_df['vote_date'] = pd.to_datetime(vote_df['vote_date'])
    vote_df['vote'] = vote_df['vote'].apply(change_vote_to_int)
    vote_df['voting_agency'] = vote_df['voting_agency'].apply(change_agency_to_int)

    return vote_df

    