import pandas as pd
import numpy as np

from data_cleaning.create_staging_tables import create_loyalty_scores_df 
from data_cleaning.edit_current_legislator import filter_out_duplicates_from_current_leg


def prepare_bill_text_for_clean_df(bill_text_df):
    '''Prepare bill_text for clean_df by lowering all text and removing \n. Add first_date_read variable
    and truncate bill text so that language about passing and votes are removed.
    
    Args:
        bill_text_df: pandas dataframe loaded from wa_leg_staging database, bill_text table

    Returns:
        bill_text_df: clean pandas dataframe
    '''

    # Cleaning and feature engineering functions
    def clean_bill_text(bill_text):
        bill_text_lower = bill_text.lower()
        bill_text_prepared = bill_text_lower.replace('\n', ' ') 
        return bill_text_prepared

    def find_read_first_time_date(bill_text):
        '''Find first_read_date from the bill text. Bills have different formatting.'''
        try:
            bill_split = bill_text.split('first time ')[1]
            end_points = range(2, 26)
            all_datetimes = []
            for end_point in end_points:
                try:
                    date = pd.Timestamp(bill_split[0:end_point])
                except:
                    date = ''
                if type(date) != str:
                    all_datetimes.append(date)
            date = all_datetimes[-1]
            return date
        except:
            return None    

    def split_off_beginning_text(bill_text):
        try:
            return (bill_text.split('an act relating'))[1]
        except:
            return bill_text

    def split_off_end_text(bill_text):
        try:
            return (bill_text.split('passed by the'))[0]
        except:
            return bill_text

    # Feature Cleaning and Engineering    
    bill_text_df['bill_text'] = bill_text_df['bill_text'].apply(clean_bill_text)       
    bill_text_df['first_date_read'] = bill_text_df['bill_text'].apply(find_read_first_time_date)
    bill_text_df['bill_text'] = bill_text_df['bill_text'].apply(split_off_beginning_text)
    bill_text_df['bill_text'] = bill_text_df['bill_text'].apply(split_off_end_text)
    return bill_text_df


def prepare_for_train_test_split(merged_final_df, bill_text_df, rep_score_df):
    """Add bill_text and rep_score and clean. Create features dem_score, bill_length, num_sections, 
    congress_num, num_sponsors, vote_year, first_year_read, years_until_vote, bill_type, bill_type_score.
    
    Args:
        merged_final_df: pandas dataframe loaded from wa_leg_staging database, merged_final table
        rep_score_df: pandas dataframe loaded from wa_leg_staging database, rep_score table
        bill_text_df: pandas dataframe loaded from wa_leg_staging database, bill_text table
    
    Returns:
        clean: a pandas dataframe ready for train_test_split
    """
    
    # Clean bill_text then add to merged_final_df
    bill_text_df_clean = prepare_bill_text_for_clean_df(bill_text_df)
    clean = merged_final_df.merge(bill_text_df, how='left', on=['unique_id', 'htm_url'])
    clean.drop(['index_x', 'index_y'], axis=1, inplace=True)
    clean.rename(columns={'sponsor_party': 'primary_sponsor_party'}, inplace=True)

    
    # Add rep_score and replace -1 values, then make dem_score
    clean['bill_num'] = clean['bill_id'].apply(lambda x: x.split()[1])
    clean['bill_num_unique'] = clean['biennium'] + ' ' + clean['bill_num']
    clean = clean.merge(rep_score_df, how='left', on='bill_num_unique')

    def replace_with_p_sponsor_party(row):
        primary_sponsor_party = row['primary_sponsor_party']
        if row['rep_score'] == -1:
            return primary_sponsor_party
        else:
            return row['rep_score']
    clean['rep_score'] = clean.apply(replace_with_p_sponsor_party, axis=1)
    clean['dem_score'] = 1 - clean['rep_score']
    
    # Feature Engineering functions
    def create_congress_nums(biennium):
        '''Numbers bienniums so that 1991 is 1 and 1993 is 3.'''
        first_year = int((biennium.split('-'))[0])
        return first_year - 1990
    
    def find_num_sponsors(secondary_sponsors):
        if type(secondary_sponsors) == str:
            return round((len(secondary_sponsors) / 5) + 1)
        else: return 1
        
    def remove_neg(num):
        if num < 0:
            return 0
        else: return num
        
    def count_sections(bill_text):
        sections = []
        for count in range(1, 20):
            section_indicator = '({})'.format(count)
            if section_indicator in bill_text:
                sections.append(count)
        if len(sections) == 0:
            return 0
        return np.max(sections)
    
    def get_bill_type(bill_id):
        bill_type = (bill_id.split(' '))[0]
        return bill_type
    
    # Feature Engineering
    clean['bill_length'] = clean['bill_text'].apply(lambda x: len(x))
    clean['num_sections'] = clean['bill_text'].apply(count_sections)
    clean['congress_num'] = clean['biennium'].apply(create_congress_nums)
    clean['num_sponsors'] = clean['secondary_sponsors'].apply(find_num_sponsors)
    
    clean['vote_year'] = clean['vote_date'].apply(lambda x: x.year)
    clean['first_year_read'] = clean['first_date_read'].apply(lambda x: x.year)
    clean['first_year_read'] = clean['first_year_read'].fillna(clean['vote_year'])
    clean['years_until_vote'] = clean['vote_year'] - clean['first_year_read']
    clean['years_until_vote'] = clean['years_until_vote'].apply(remove_neg)
    
    clean['bill_type'] = clean['bill_id'].apply(get_bill_type)
    grouped = clean.groupby('bill_type').mean()
    bill_type_dct = grouped['vote'].to_dict()
    clean['bill_type_score'] = clean['bill_type'].apply(lambda x: bill_type_dct[x])
    
    return clean



def create_sample_train_test(clean, n=300000):
    """Create smaller sample of entire dataset to work with for training and testing model. Filter out all 
    undecided or absent votes. Train data consists of all votes before July 1, 2017, testing data consists
    of all votes after July 1, 2017. loyaly_score and percent_yea are calculated on train set only.
    
    Args:
        n (int): number of rows in sample
    Returns:
        sample_train: train set pandas dataframe ready for modeling
        sample_test: test set pandas dataframe ready for modeling
        
    """
    sample = clean.sample(n=500000)
    sample = sample[sample['vote'] <= 1]
    
    sample_train = sample[sample['vote_date'] < '2017-07-01T00:00:00.000000000']
    sample_test = sample[sample['vote_date'] > '2017-07-01T00:00:00.000000000']
    
    loyalty_scores_df = create_loyalty_scores_df(sample_train)
    
    # Add loyalty scores to train
    sample_train = sample_train.merge(loyalty_scores_df, how = 'left', on='voter_id')
    sample_train = sample_train.drop('index', axis=1)

    # Determine party averages
    loy_dem_average = np.mean(sample_train[sample_train['party'] == 0]['loyalty_score'])
    loy_rep_average = np.mean(sample_train[sample_train['party'] == 1]['loyalty_score'])
    yea_dem_average = np.mean(sample_train[sample_train['party'] == 0]['percent_yea'])
    yea_rep_average = np.mean(sample_train[sample_train['party'] == 1]['percent_yea'])
    
    # Add loyalty scores to test
    sample_test = sample_test.merge(loyalty_scores_df, how = 'left', on='voter_id')
    sample_test = sample_test.drop('index', axis=1)
    
    def fillna_loyalty(row):
        if row['loyalty_score'] == 1000:
            if row['party'] == 0:
                return loy_dem_average
            if row['party'] ==1:
                return loy_rep_average
        else:
            return row['loyalty_score']
    
    def fillna_percent_yea(row):
        if row['percent_yea'] == 1000:
            if row['party'] == 0:
                return yea_dem_average
            if row['party'] ==1:
                return yea_rep_average
        else:
            return row['percent_yea']
    

    sample_test['loyalty_score'] = sample_test['loyalty_score'].fillna(1000)
    sample_test['percent_yea'] = sample_test['loyalty_score'].fillna(1000)
    sample_test['loyalty_score'] = sample_test.apply(fillna_loyalty, axis=1)
    sample_test['percent_yea'] = sample_test.apply(fillna_percent_yea, axis=1)
    
    return sample_train, sample_test



def create_clean_train(merged_final_df, bill_text_df, rep_score_df, loyalty_df):
    """Add bill_text and rep_score and clean. Create features dem_score, bill_length, num_sections, 
    congress_num, num_sponsors, vote_year, first_year_read, years_until_vote, bill_type, bill_type_score.
    
    Args:
        merged_final_df: pandas dataframe loaded from wa_leg_staging database, merged_final table
        bill_text_df: pandas dataframe loaded from wa_leg_staging database, bill_text table
        rep_score_df: pandas dataframe loaded from wa_leg_staging database, rep_score table
        loyalty_df: pandas dataframe loaded from wa_leg_staging database, loyalty table
    
    
    Returns:
        clean: pandas dataframe ready for training for final predictions
        bill_type_dct: dictionary containing unique bill types and their corresponding bill_type_score
    
    """
    
    # Add bill_text
    bill_text_df_clean = prepare_bill_text_for_clean_df(bill_text_df)
    clean = merged_final_df.merge(bill_text_df_clean, how='left', on=['unique_id', 'htm_url'])
    clean.drop(['index_x', 'index_y'], axis=1, inplace=True)
    clean.rename(columns={'sponsor_party': 'primary_sponsor_party'}, inplace=True)

    
    # Add rep_score and replace -1 values, then make dem_score
    clean['bill_num'] = clean['bill_id'].apply(lambda x: x.split()[1])
    clean['bill_num_unique'] = clean['biennium'] + ' ' + clean['bill_num']
    clean = clean.merge(rep_score_df, how='left', on='bill_num_unique')

    def replace_with_p_sponsor_party(row):
        primary_sponsor_party = row['primary_sponsor_party']
        if row['rep_score'] == -1:
            return primary_sponsor_party
        else:
            return row['rep_score']
    clean['rep_score'] = clean.apply(replace_with_p_sponsor_party, axis=1)
    clean['dem_score'] = 1 - clean['rep_score']
    
    
    # Add loyalty scores
    clean = clean.merge(loyalty_df, how = 'left', on='voter_id')
    clean.drop('index', axis=1, inplace=True)
    
    
    # Feature Engineering functions
    def create_congress_nums(biennium):
        '''Numbers bienniums so that 1991 is 1 and 1993 is 3.'''
        first_year = int((biennium.split('-'))[0])
        return first_year - 1990
    
    def find_num_sponsors(secondary_sponsors):
        if type(secondary_sponsors) == str:
            return round((len(secondary_sponsors) / 5) + 1)
        else: return 1
        
    def remove_neg(num):
        if num < 0:
            return 0
        else: return num
        
    def count_sections(bill_text):
        sections = []
        for count in range(1, 20):
            section_indicator = '({})'.format(count)
            if section_indicator in bill_text:
                sections.append(count)
        if len(sections) == 0:
            return 0
        return np.max(sections)
    
    def get_bill_type(bill_id):
        bill_type = (bill_id.split(' '))[0]
        return bill_type
    
    # Feature Engineering
    clean['bill_length'] = clean['bill_text'].apply(lambda x: len(x))
    clean['num_sections'] = clean['bill_text'].apply(count_sections)
    clean['congress_num'] = clean['biennium'].apply(create_congress_nums)
    clean['num_sponsors'] = clean['secondary_sponsors'].apply(find_num_sponsors)
    
    clean['vote_year'] = clean['vote_date'].apply(lambda x: x.year)
    clean['first_year_read'] = clean['first_date_read'].apply(lambda x: x.year)
    clean['first_year_read'] = clean['first_year_read'].fillna(clean['vote_year'])
    clean['years_until_vote'] = clean['vote_year'] - clean['first_year_read']
    clean['years_until_vote'] = clean['years_until_vote'].apply(remove_neg)
    
    clean['bill_type'] = clean['bill_id'].apply(get_bill_type)
    grouped = clean.groupby('bill_type').mean()
    bill_type_dct = grouped['vote'].to_dict()
    clean['bill_type_score'] = clean['bill_type'].apply(lambda x: bill_type_dct[x])

    return clean, bill_type_dct



def create_clean_test(current_df, current_bill_text_df, loyalty_df, bill_type_dct):
    """Add bill_text and rep_score and clean. Create features dem_score, bill_length, num_sections, 
    congress_num, num_sponsors, vote_year, first_year_read, years_until_vote, bill_type, bill_type_score.
    
    Args:
        current_df: pandas dataframe loaded from wa_leg_staging database, current table
        current_bill_text_df: pandas dataframe loaded from wa_leg_staging database, current_bill_text table
        loyalty_df: pandas dataframe loaded from wa_leg_staging database, loyalty table
        bill_type_dct (dict): dictionary created in create_clean_train
    
    
    Returns:
        clean: pandas dataframe ready for transforming to final predictions
        bill_type_dct: dictionary containing unique bill types and their corresponding bill_type_score
    """
    
    # Add bill_text
    current_bill_text_df_clean = prepare_bill_text_for_clean_df(current_bill_text_df)
    clean = current_df.merge(current_bill_text_df, how='left', on=['bill_id', 'biennium', 'htm_url'])
    clean.rename(columns={'sponsor_party': 'primary_sponsor_party'}, inplace=True)

    
    # Make dem_score
    clean['dem_score'] = 1 - clean['rep_score']
    
    
    # Add loyalty scores
    clean = clean.merge(loyalty_df, how = 'left', on='voter_id')
    
    # Feature Engineering functions
    def create_congress_nums(biennium):
        '''Numbers bienniums so that 1991 is 1 and 1993 is 3.'''
        first_year = int((biennium.split('-'))[0])
        return first_year - 1990
    
    def find_num_sponsors(secondary_sponsors):
        if type(secondary_sponsors) == str:
            return round((len(secondary_sponsors) / 5) + 1)
        else: return 1
        
    def remove_neg(num):
        if num < 0:
            return 0
        else: return num
        
    def count_sections(bill_text):
        sections = []
        for count in range(1, 20):
            section_indicator = '({})'.format(count)
            if section_indicator in bill_text:
                sections.append(count)
        if len(sections) == 0:
            return 0
        return np.max(sections)
    
    def get_bill_type(bill_id):
        bill_type = (bill_id.split(' '))[0]
        return bill_type
    
    # Feature Engineering
    clean['bill_length'] = clean['bill_text'].apply(lambda x: len(x))
    clean['num_sections'] = clean['bill_text'].apply(count_sections)
    clean['congress_num'] = clean['biennium'].apply(create_congress_nums)

    clean['vote_year'] = 2018
    clean['first_year_read'] = clean['first_date_read'].apply(lambda x: x.year)
    clean['first_year_read'] = clean['first_year_read'].fillna(clean['vote_year'])
    clean['years_until_vote'] = clean['vote_year'] - clean['first_year_read']
    clean['years_until_vote'] = clean['years_until_vote'].apply(remove_neg)
    
    # Filter out duplicate legislators
    clean['current'] = clean.apply(filter_out_duplicates_from_current_leg, axis=1)
    clean = clean[clean['current'] == True]
    
    clean['bill_type'] = clean['bill_id'].apply(get_bill_type)
    clean['bill_type_score'] = clean['bill_type'].apply(lambda x: bill_type_dct[x])

    return clean



def create_label_df(test_clean, y_pred_t, current_legislator_df):
    """Take the labels predicted from the top model and add them with legislator information and the test_clean
    table to create a table full of all the info needed for the website.""
    
    Args:
        test_clean: data frame created with creat_test_clean
        y_pred_t: numpy array created after running test_clean through a model
        current_legislator_df: pandas dataframe loaded from wa_leg_staging database, current_legislator table
    
    Returns:
        label_df: dataframe with all necessary information for website
    """
    test_clean['predicted_vote'] = y_pred_t
    label_df = test_clean[['bill_id', 'bill_num', 'biennium', 'voter_id', 'voting_agency', 'predicted_vote', 
                       'rep_score', 'htm_url', 'secondary_sponsors']]
    label_df = label_df.merge(current_legislator_df, how='left', on=['voter_id', 'voting_agency'])
    label_df = label_df[label_df['last_name'].notnull()]
    return label_df