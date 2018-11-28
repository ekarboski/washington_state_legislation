import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from sklearn.base import BaseEstimator, TransformerMixin
from collections import defaultdict
from web_scrape_functions import scrape_bill_url


def create_staging_legislator_df_STEP_ONE(vote_df, committee_member_df, missing_leg_info_df):
    """Merge votes_df, committee_df and missing_legislator_info_df and clean data to create legislator_df
    
    Input
    votes_df: pandas dataframe loaded from wa_leg_raw database, vote_api table
    committee_member_df: pandas dataframe loaded from wa_leg_raw database, committee_member_api table
    missing_legislator_info_df: pandas dataframe loaded missing_legislators.csv
    """
    leg_info_from_vote_df = vote_df.loc[:, ['biennium', 'voter_id', 'voter_name', 'voting_agency']]
    leg_info_from_vote_df.drop_duplicates(keep='first', inplace=True)
    
    leg_info_from_cm_df = committee_member_df.loc[:, ['district', 'id', 'name', 'first_name', 'party']]
    leg_info_from_cm_df = leg_info_from_cm_df.rename(index=str, columns={"id": "voter_id"})
    leg_info_from_cm_df['last_name'] = leg_info_from_cm_df['name'].apply(lambda x: x.split()[1])
    leg_info_from_cm_df.drop_duplicates(keep='first', inplace=True)

    legislator_df = leg_info_from_vote_df.merge(leg_info_from_cm_df, how='outer', on='voter_id')
    legislator_df.drop_duplicates(keep="first", inplace=True)
    
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
        
        
      
def create_staging_vote_df_STEP_TWO(vote_df):
    '''Create statging_vote_df from raw_vote_df'''

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





def create_secondary_sponsor_column(sponsor_df):
    '''Create a column named secondary_sponsors that contains a list of secondary sponsors
    
    Input:
    sponsor_df: pandas dataframe retrieved from wa_leg raw database, sponsor_api table
    '''
    # Create dictionary with bill_id and biennium as keys and list of secondary sponsors as values
    s = defaultdict(list)
    for s_id, s_type, biennium, bill_id in zip(sponsor_df['sponsor_id'], 
                                               sponsor_df['sponsor_type'],
                                               sponsor_df['biennium'], 
                                               sponsor_df['bill_id']):
        if s_type == 'Secondary':
            s[(bill_id, biennium)].append(s_id)
            
    for k, v in s.items():
        s[k] = list(set(v))

    # Create a matrix that can be turned in to a dataframe. Column one is bill_id, column two is 
    # biennium, column three is the list of secondary sponsors
    sponsor_matrix = []
    for k, v in s.items():
        row = []
        row.append(k[0])
        row.append(k[1])
        row.append(v)
        sponsor_matrix.append(row)
        
    secondary_sponsors_df = pd.DataFrame(sponsor_matrix)
    secondary_sponsors_df.columns = ['bill_id', 'biennium', 'secondary_sponsors']
            
    sponsor_df_reformatted = sponsor_df[sponsor_df['sponsor_type'] == 'Primary']
    sponsor_df_reformatted['bill_num'] = sponsor_df_reformatted['bill_id'].apply(lambda x: x.split()[1])
    sponsor_df_reformatted['bill_num_unique'] = sponsor_df_reformatted['biennium'] + ' ' + sponsor_df_reformatted['bill_num']
    sponsor_df_reformatted = sponsor_df_reformatted.rename(index=str, columns={"sponsor_id": "primary_sponsor_id"})
    sponsor_df_reformatted = sponsor_df_reformatted.drop(['sponsor_type', 'sponsor_order', 'sponsor_last_name', 
                                 'sponsor_long_name', 'sponsor_first_name', 'sponsor_name'], axis = 1)
    
    sponsor_df_merged = sponsor_df_reformatted.merge(secondary_sponsors_df, how='outer', on=['bill_id', 'biennium'])
    
    # filter out repeating bills
    unique_bill_nums = []
    unique_bill_rows = []
    for i, row in sponsor_df_merged.iterrows():
        if row['bill_num_unique'] not in unique_bill_nums:
            unique_bill_nums.append(row['bill_num_unique'])
            unique_bill_rows.append(row)
            
    return pd.DataFrame(unique_bill_rows)


def create_staging_bill_df_STEP_THREE(bill_df, sponsor_df):
    '''Join sponsor_df to bill_df and output the merged pandas dataframe.
    Input
    bill_df: pandas dataframe retrieved from wa_leg raw database, bill_api table
    sponsor_df: pandas dataframe retrieved from wa_leg raw database, sponsor_api table
    '''
    sponsor_df_unique = create_secondary_sponsor_column(sponsor_df)
    bill_df['bill_num'] = bill_df['bill_id'].apply(lambda x: x.split()[1] if type(x) == str else x)
    bill_df['bill_num_unique'] = bill_df['biennium'] + ' ' + bill_df['bill_num']
    
    MERGED = bill_df.merge(sponsor_df_unique, how='left', on=['bill_num_unique', 'biennium'], suffixes=['', '_sp'])
    MERGED = MERGED.drop(['bill_id_sp', 'bill_num_sp'], axis=1)
    MERGED = MERGED[MERGED['primary_sponsor_id'].notnull()]
    return MERGED


def create_staging_unique_vote_dates_df(staging_vote_df, staging_bill_df):
    '''Identify the exact bill that the legislators voted on. Create a dataframe that conists of all vote_date 
    and bill_unique pairs. Create a null unique_id field that labels each bill with a unique ID, and begins as 
    null for unique_vote_dates. For each row in unique_vote_dates identify the bill that was created closest to, 
    but before, the vote date. Set the unique_id of that bill to the unique_id of that vote. This will be use to 
    later join the bill_df to the vote_df.'''
    
    staging_bill_df['htm_create_date'] =        pd.to_datetime(staging_bill_df['htm_create_date'])
    staging_bill_df['htm_last_modified_date'] = pd.to_datetime(staging_bill_df['htm_last_modified_date'])
    
    unique_vote_dates = staging_vote_df[['bill_unique', 'vote_date']]
    unique_vote_dates.drop_duplicates(keep='first', inplace=True)
    unique_vote_dates = unique_vote_dates.reset_index()
    
    unique_vote_dates['unique_id'] = np.nan
    staging_bill_df['unique_id'] = np.linspace(1, len(staging_bill_df), len(staging_bill_df))
    

    for i1, row in unique_vote_dates.iterrows():
        time_diffs = {}
        bill_options = staging_bill_df[staging_bill_df['bill_unique'] == row['bill_unique']]
        
        for i2, option in bill_options.iterrows():
            time_diff = option['htm_create_date'] - row['vote_date']
            
            if time_diff <= pd.to_timedelta('0'):
                time_diffs[time_diff] = option['unique_id']
            if time_diff > pd.to_timedelta('0'):
                time_diff -= pd.to_timedelta('-1000 days')
                time_diffs[time_diff] = option['unique_id']
                
        if len(time_diffs) > 0:
            bill_voted_on = time_diffs[max(time_diffs)]
            unique_vote_dates.iloc[i1, -1] = bill_voted_on
            
    return unique_vote_dates, staging_bill_df

def create_staging_merged_initial_df(staging_vote_df, staging_bill_df):
    '''Create merged_initial by merging staging_bill_df and staging_vote_df on the unique_id field created in 
    create_staging_unique_vote_dates_df.
    Input
    staging_bill_df: pandas dataframe loaded from wa_leg_staging database, bill table
    staging_vote_df:pandas dataframe loaded from wa_leg_staging database, vote table
    '''
    unique_vote_dates, staging_bill_df_with_unique_ids = create_staging_unique_vote_dates_df(staging_vote_df, staging_bill_df)
    unique_vote_dates = unique_vote_dates.drop('index', axis=1)
    staging_vote_df_with_unique_ids = staging_vote_df.merge(unique_vote_dates, how='left', on=['bill_unique', 'vote_date'])
    return staging_vote_df_with_unique_ids.merge(staging_bill_df_with_unique_ids, how='left', on=['unique_id', 'bill_unique'])


def create_staging_bill_text_df_STEP_FOUR(merged_initial_df):
    '''Create staging_bill_text using unique bills from merged_initial_df and scraping the urls.
    Input
    merged_initial_df: pandas dataframe loaded from wa_leg_staging database, merged_initial table
    '''
    
    bills_to_scrape_df = merged_initial_df[['unique_id', 'htm_url']]
    bills_to_scrape_df.drop_duplicates(keep='first', inplace=True)
    bills_to_scrape_df['bill_text'] = ''
    bills_to_scrape_df.reset_index(inplace=True)
    
    for i, row in bills_to_scrape_df.iterrows():
        url = row['htm_url']
        try: 
            bill_text = scrape_bill_url(url)
            bills_to_scrape_df.iloc[i,-1] = bill_text
        except:
            continue
    return bills_to_scrape_df


def create_staging_merged_final_df_STEP_FIVE(staging_vote_df, staging_bill_df, legislator_df):
    '''Create merged_final pandas dataframe. This dataframe contains all necessary data and is ready for 
    cleaning.
    Input
    merged_initial_df: pandas dataframe loaded from wa_leg_staging database, merged_inital table
    bill_text_df: pandas dataframe loaded from wa_leg_staging database, bill_text table
    legislator_df: pandas dataframe loaded from wa_leg_staging database, legislator table
    '''
    merged_initial_df = create_staging_merged_initial_df(staging_vote_df, staging_bill_df)
    # merged_second_df = merged_initial_df.merge(bill_text_df, how='left', on=['unique_id', 'htm_url'])
    
    def change_agency_to_int(agency):
        if agency == 'House':
            return 0
        if agency == 'Senate':
            return 1
    
    legislator_df['agency'] = legislator_df['agency'].apply(change_agency_to_int)
    merged_final = merged_initial_df.merge(legislator_df, 
                               how='left', 
                               left_on=['voter_id', 'voting_agency'], 
                               right_on=['id', 'agency'])
    
    merged_final = merged_final.drop(['sequence_number', 'type', 
                                      'voter_name', 'htm_last_modified_date', 'description', 
                                      'bill_num_unique', 'bill_num', 'class'], axis = 1)
    return merged_final
    
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


def clean_merged_final_STEP_SIX(MERGED_final):
    '''Create a MERGED_final pandas dataframe and clean columns so that data type is correct. Feature
    engineer several new features such as is_primary_sponsor.
    
    Parameters
    ----------
    MERGED_final: pandas dataframe created with create_staging_merged_final_df_STEP_FIVE

    Output
    ------
    clean_df: pandas dataframe
    '''
    
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
        if len(subset_leg) > 0:
            sponsor_party = subset_leg.iloc[0, 4]
            return sponsor_party
        else: return 2

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