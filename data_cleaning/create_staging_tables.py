import psycopg2
from sqlalchemy import create_engine
import pandas as pd
import numpy as np
from collections import defaultdict
import urllib
from bs4 import BeautifulSoup
# from data_aquisition.web_scrape_functions import scrape_bill_url

def create_staging_legislator_df_STEP_ONE(vote_df, committee_member_df, missing_leg_info_df):
    """Merge vote_df, committee_df and missing_legislator_info_df and clean data to create legislator_df.
    
    Args:
        vote_df: pandas dataframe loaded from wa_leg_raw database, vote_api table
        committee_member_df: pandas dataframe loaded from wa_leg_raw database, committee_member_api table
        missing_legislator_info_df: pandas dataframe loaded from missing_legislators.csv

    Returns:
        legislator_df: pandas dataframe with district, party, id, names and agency information for each legislator
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
        
        
      
def create_staging_vote_df_STEP_TWO(raw_vote_df):
    """Create statging_vote_df from raw_vote_df by cleaning features and changing data types.
    
    Args:
        raw_vote_df: pandas dataframe loaded from wa_leg_raw database, vote_api table
    
    Returns:
        staging_vote_df: pandas dataframe
    """

    raw_vote_df['bill_unique'] = raw_vote_df['biennium'] + ' ' + raw_vote_df['bill_id']


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

    raw_vote_df['year'] = raw_vote_df['biennium'].apply(change_biennium_to_year)
    staging_vote_df = raw_vote_df.drop(['bill_id', 'biennium', 'motion'], axis=1)
    staging_vote_df['sequence_number'] = staging_vote_df['sequence_number'].apply(int)
    staging_vote_df['voter_id'] = staging_vote_df['voter_id'].apply(int)
    staging_vote_df['vote_date'] = pd.to_datetime(staging_vote_df['vote_date'])
    staging_vote_df['vote'] = staging_vote_df['vote'].apply(change_vote_to_int)
    staging_vote_df['voting_agency'] = staging_vote_df['voting_agency'].apply(change_agency_to_int)

    return staging_vote_df


def create_staging_bill_df_STEP_THREE(raw_bill_df, sponsor_df):
    """Merge raw_sponsor_df to raw_bill_df to add sponsor features to bill_df.
  
    Args:
        bill_df: pandas dataframe retrieved from wa_leg_raw database, bill_api table
        sponsor_df: pandas dataframe retrieved from wa_leg_raw database, sponsor_api table

    Returns:
        staging_bill_df: pandas dataframe 
    """
    sponsor_df_unique = create_secondary_sponsor_column(sponsor_df)
    raw_bill_df['bill_unique'] = raw_bill_df['biennium'] + ' ' + raw_bill_df['bill_id']
    raw_bill_df['bill_num'] = raw_bill_df['bill_id'].apply(lambda x: x.split()[1] if type(x) == str else x)
    raw_bill_df['bill_num_unique'] = raw_bill_df['biennium'] + ' ' + raw_bill_df['bill_num']
    
    staging_bill_df = raw_bill_df.merge(sponsor_df_unique, how='left', on=['bill_num_unique', 'biennium'], suffixes=['', '_sp'])
    staging_bill_df = staging_bill_df.drop(['bill_id_sp', 'bill_num_sp'], axis=1)
    staging_bill_df = staging_bill_df[staging_bill_df['primary_sponsor_id'].notnull()]
    return staging_bill_df


def create_staging_merged_initial_df_STEP_FOUR(staging_vote_df, staging_bill_df, legislator_df):
    """Create merged_initial by merging staging_bill_df, staging_vote_df and legislator_df.
    
    Args:
        staging_bill_df: pandas dataframe loaded from wa_leg_staging database, bill table
        staging_vote_df:pandas dataframe loaded from wa_leg_staging database, vote table
        legislator_df: pandas dataframe loaded from wa_leg_staging database, legislator table

    Returns:
        merged_initial_df: pandas dataframe
    """

    unique_vote_dates, staging_bill_df_with_unique_ids = create_staging_unique_vote_dates_df(staging_vote_df, staging_bill_df)
    unique_vote_dates = unique_vote_dates.drop('index', axis=1)
    staging_vote_df_with_unique_ids = staging_vote_df.merge(unique_vote_dates, how='left', on=['bill_unique', 'vote_date'])
    merged_initial_df = staging_vote_df_with_unique_ids.merge(staging_bill_df_with_unique_ids, how='left', on=['unique_id', 'bill_unique'])

    legislator_df['agency'] = legislator_df['agency'].apply(change_agency_to_int)
    merged_initial_df['sponsor_agency'] = merged_initial_df['sponsor_agency'].apply(change_agency_to_int)
    merged_initial_df = merged_initial_df.merge(legislator_df, 
                               how='left', 
                               left_on=['voter_id', 'voting_agency'], 
                               right_on=['id', 'agency'])
    
    merged_initial_df = merged_initial_df.drop(['sequence_number', 'type', 
                                      'voter_name', 'htm_last_modified_date', 'description', 
                                      'bill_num_unique', 'bill_num', 'class'], axis = 1)
    merged_initial_df['party'] = merged_initial_df['party'].apply(change_party_word_to_int)
    return merged_initial_df



def create_staging_bill_text_df_STEP_FIVE(merged_initial_df):
    """Create staging_bill_text using unique bills from merged_initial_df and scraping the urls.

    Args:
        merged_initial_df: pandas dataframe loaded from wa_leg_staging database, merged_initial table

    Returns:
        bill_text_scrape_df: pandas dataframe
    """
    
    bill_text_scrape_df = merged_initial_df[['unique_id', 'htm_url']]
    bill_text_scrape_df.drop_duplicates(keep='first', inplace=True)
    bill_text_scrape_df['bill_text'] = ''
    bill_text_scrape_df.reset_index(inplace=True)
    
    for i, row in bill_text_scrape_df.iterrows():
        url = row['htm_url']
        try: 
            bill_text = scrape_bill_url(url)
            bill_text_scrape_df.iloc[i,-1] = bill_text
        except:
            continue
    return bill_text_scrape_df


def clean_merged_final_STEP_SIX(merged_initial_df, legislator_df):
    """Create a merged_intermediate pandas dataframe and clean columns so that data type is correct. Feature
    engineer several new features such as is_primary_sponsor.
    
    Args: 
        merged_initial_df: pandas dataframe created with create_staging_merged_initial_df_STEP_FOUR
        legislator_df: pandas dataframe loaded from wa_leg_staging, legislator table

    Returns:
        merged_final: pandas dataframe
    """
    
    merged_valid = merged_initial_df[merged_initial_df['primary_sponsor_id'].notnull()]
    merged_final = merged_valid.drop(['name', 'first_name', 'last_name', 'id', 
                                  'agency', 'long_friendly_name'], axis=1)
    
    merged_final['sponsor_agency'] = merged_final['sponsor_agency'].apply(change_agency_to_int)
    merged_final['primary_sponsor_id'] = merged_final['primary_sponsor_id'].apply(int)
    # merged_final['secondary_sponsors'] = merged_final['secondary_sponsors'].apply(make_sec_sponsors_a_list)
    merged_final['secondary_sponsors'].fillna('', inplace=True)
    merged_final['is_primary_sponsor'] = merged_final['voter_id'] == merged_final['primary_sponsor_id']
    
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
    
    merged_final['is_minority_party'] = merged_final.apply(make_is_minority_party, axis=1)
    merged_final['is_secondary_sponsor'] = merged_final.apply(make_is_secondary_sponsor, axis=1)
    merged_final['sponsor_party'] = merged_final.apply(find_sponsor_party, axis=1)
    merged_final = merged_final.drop(['primary_sponsor_id', 'secondary_sponsors', 'htm_create_date', 'year'], axis=1)
    
    return merged_final



def create_rep_score_STEP_SEVEN(staging_bill_df, legislator_df):
    """Create a dataframe with bill_num_unique, secondary_sponsors, primary_sponsor_id and rep_score.
    rep_score is a ratio of number of republican sponsors (primary and secondary) to total number of sponsors. 

    Args:
        staging_bill_df: pandas dataframe loaded from wa_leg_staging database, bill table
        legislator_df: pandas dataframe loaded from wa_leg_staging database, legislator table
        
    Returns:
        rep_score_df: pandas dataframe"""

    rep_score_df = staging_bill_df[['secondary_sponsors', 'bill_num_unique', 'primary_sponsor_id']]
    rep_score_df.drop_duplicates(keep='first', inplace=True)
    # rep_score_df['secondary_sponsors'] = rep_score_df['secondary_sponsors'].apply(make_sec_sponsors_a_list)
    rep_score_df['rep_score'] = rep_score_df.apply(create_bill_rep_score, axis=1)

    return rep_score_df









# CLEANING FUNCTIONS
def scrape_bill_url(url):
    """Scrape all text from the bill url and put into one string.
    
    Args:
        url (str): url provided by bill_api
        
    Returns:
        text (str): text of bill    
        """

    url = url.replace(' ', '%20')
    html = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(html, "lxml")
    for script in soup(["script", "style"]):
        script.extract()
    raw_text = soup.get_text()
    lines = [l.strip() for l in raw_text.splitlines()]
    phrases = [phrase.strip() for line in lines for phrase in line.split("  ")]
    text = '\n'.join([phrase for phrase in phrases])
    return text

def change_agency_to_int(agency):
    if agency == 'House':
        return 0
    if agency == 'Senate':
        return 1

def make_sec_sponsors_a_list(sponsors):
        '''Secondary sponsors are in a string. The string will be split in to a list of sponsor ids.'''
        if type(sponsors) == str:
            clean_sponsors = sponsors.strip('{}')
            id_list = clean_sponsors.split(',')
            return id_list
        else: 
            return sponsors

def change_party_word_to_int(party):
        if party == 'Democrat':
            return 0
        if party == 'Republican':
            return 1

def create_bill_rep_score(row):
    '''Return # of republican sponsors / total sponsors. If there are no secondary sponsors return -1 so that
    later, when this table is joined with merged_final, the primary sponsor party can fill this field.'''
    sponsors = row['secondary_sponsors']
    if type(sponsors) == list:
        sponsors.append(row['primary_sponsor_id'])

        sponsor_parties = []
        for s in sponsors:
            subset_leg = legislator_df[(legislator_df['id'] == int(s))]
            if len(subset_leg) > 0:
                sponsor_party = sum(subset_leg.iloc[:, 4]) / len(subset_leg)
                sponsor_parties.append(sponsor_party)
        bill_rep_score = np.mean(sponsor_parties)
        return bill_rep_score
    else:
        return -1


def create_secondary_sponsor_column(sponsor_df):
    """Reorganizes raw_sponsor_df to create a column named secondary_sponsors that contains a list of secondary sponsors.
    
    Args:
        sponsor_df: pandas dataframe retrieved from wa_leg_raw database, sponsor_api table

    Returns:
        vote_df: pandas dataframe 
    """
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


def create_staging_unique_vote_dates_df(staging_vote_df, staging_bill_df):
    """Identify the exact bill that the legislators voted on. Create a dataframe that conists of all vote_date 
    and bill_unique pairs. Create a unique_id field that labels each bill with a unique ID, and begins as 
    null for unique_vote_dates. For each row in unique_vote_dates identify the bill that was created closest to, 
    but before, the vote date. Set the unique_id of that bill to the unique_id of that vote. This will be use to 
    later join the staging_bill_df to the staging_vote_df.
    
    Args:
        staging_vote_df: pandas dataframe retrieved from wa_leg_staging database, vote table
        staging_bill_df: pandas dataframe retrieved from wa_leg_staging database, bill table

    Returns:
        unique_vote_dates: pandas dataframe of unique bills with 'unique_id' feature
        staging_bill_df: pandas dataframe of bill info with matching 'unique_id' feature
    """
    
    staging_bill_df['htm_create_date'] = pd.to_datetime(staging_bill_df['htm_create_date'])
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


def load_and_clean_party_minority_history_df():
    '''Load party minority history data from csv file and make agency and minority_party fields ints.'''

    minority_hist = pd.read_csv('../data/party_minority_history.csv', sep="|")

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


def create_loyalty_scores_df(merged_final):
    """Calculate loyalty scores and percent_yea for each representative. 

    Args:
        merged_final: pandas dataframe loaded from wa_leg_staging database, merged_final table
        
    Returns:
        loyalty_df: pandas dataframe
    """

    all_voters = merged_final['voter_id'].unique()
    loyalty_scores_list = []
    
    for voter in all_voters:
        loyalty_scores_dct = {}
        loyalty_scores_dct['voter_id'] = voter
        votes = merged_final[merged_final['voter_id'] == voter]
        loyalty_scores_dct['percent_yea'] = np.mean(votes['vote'])
        loyalties = []
        for i, row in votes.iterrows():
            weight = np.absolute(row['rep_score'] - row['dem_score'])
            leg_party = row['party']
            bill_party = round(row['rep_score'])

            if leg_party == bill_party:
                voted_with_party = 1
            else:
                voted_with_party = -1

            loyalty = weight * voted_with_party
            loyalties.append(loyalty)
        loyalty_score = np.mean(loyalties)
        loyalty_scores_dct['loyalty_score'] = loyalty_score
        loyalty_scores_list.append(loyalty_scores_dct)
        
    return pd.DataFrame(loyalty_scores_list)



def create_staging_current_bill_text_df(current_bill_df):
    """Create staging_current_bill_text using unique bills from current_bill_df_df and scraping the urls.

    Args:
        current_bill_df: pandas dataframe loaded from wa_leg_raw database, current_bill table
        
    Returns:
        current_bill_text_df: pandas dataframe
    """
    
    current_bill_text_df = current_bill_df[['biennium', 'bill_id', 'htm_url']]
    current_bill_text_df['bill_text'] = ''
    
    for i, row in current_bill_text_df.iterrows():
            url = row['htm_url']
            try: 
                bill_text = scrape_bill_url(url)
                current_bill_text_df.iloc[i,-1] = bill_text
            except:
                continue
    return current_bill_text_df



# These functions will be transfered to classes. This is an example of such class.

# class CreateStagingLegislatorDataframe(BaseEstimator, TransformerMixin):
    # """Merge votes_df, committee_df and missing_legislator_df and clean data to create legislator_df"""
    
    # def __init__(self, vote_df, committee_member_df, missing_leg_info_df):
    #     """Input
    #        votes_df: pandas dataframe loaded from wa_leg_raw database, vote_api table
    #        committee_member_df: pandas dataframe loaded from wa_leg_raw database, committee_member_api table
    #        missing_legislator_df: pandas dataframe loaded missing_legislators.csv"""
    #     self.vote_df = vote_df
    #     self.committee_member_df = committee_member_df
    #     self.missing_leg_info_df = missing_leg_info_df
    
    # def fit(self, X=None, y=None):
    #     return self

    # def transform(self, X=None):
    #     leg_info_from_vote_df = self.vote_df.loc[:, ['biennium', 'voter_id', 'voter_name', 'voting_agency']]
    #     leg_info_from_vote_df.drop_duplicates(keep='first', inplace=True)

    #     leg_info_from_cm_df = self.committee_member_df.loc[:, ['district', 'id', 'name', 'first_name', 'party']]
    #     leg_info_from_cm_df = leg_info_from_cm_df.rename(index=str, columns={"id": "voter_id"})
    #     leg_info_from_cm_df['last_name'] = leg_info_from_cm_df['name'].apply(lambda x: x.split()[1])
    #     leg_info_from_cm_df.drop_duplicates(keep='first', inplace=True)

    #     legislator_df = leg_info_from_vote_df.merge(leg_info_from_cm_df, how='outer', on='voter_id')
    #     legislator_df.drop_duplicates(keep="first", inplace=True)

    #     self.missing_leg_info_df['district'] = self.missing_leg_info_df['district'].apply(str)
    #     self.missing_leg_info_df['voter_id'] = self.missing_leg_info_df['voter_id'].apply(str)

    #     legislator_df = legislator_df.merge(self.missing_leg_info_df, how='outer', on='voter_id', suffixes=('', '_missing'))

    #     legislator_df['first_name'] = legislator_df.apply(lambda x : replace_missing_first_name(x),axis=1)
    #     legislator_df['party']      = legislator_df.apply(lambda x : replace_missing_party(x),     axis=1)
    #     legislator_df['district']   = legislator_df.apply(lambda x : replace_missing_district(x),  axis=1)
    #     legislator_df['last_name']  = legislator_df.apply(lambda x : replace_missing_last_name(x), axis=1)

    #     legislator_df = legislator_df.drop(['voter_name', 'name', 'district_missing', 'party_missing', 'first_name_missing', 'last_name_missing', 'biennium'], axis=1)
    #     legislator_df.drop_duplicates(keep='first', inplace=True)
    #     legislator_df.columns = ['id', 'agency', 'district', 'first_name', 'party', 'last_name']
    #     legislator_df['id'] = legislator_df['id'].apply(int)
    #     legislator_df['district'] = legislator_df['district'].apply(int)

    #     return legislator_df
    
    # @staticmethod
    # def replace_missing_first_name(x):
    #     if type(x['first_name']) == float:
    #         return x['first_name_missing']
    #     else:
    #         return x['first_name']
        
    # @staticmethod
    # def replace_missing_last_name(x):
    #     if type(x['last_name']) == float:
    #         return x['last_name_missing']
    #     else:
    #         return x['last_name']
        
    # @staticmethod
    # def replace_missing_party(x):
    #     if type(x['party']) == float:
    #         if x['party_missing'] == 'R':
    #             return 'Republican'
    #         if x['party_missing'] == 'D':
    #             return 'Democrat'
    #     else:
    #         return x['party']
        
        
    # @staticmethod
    # def replace_missing_district(x):
    #     if type(x['district']) == float:
    #         return x['district_missing']
    #     else:
    #         return x['district']