import pandas as pd
import numpy as np


def prepare_bill_text_for_clean_df(bill_text_df):
    '''Prepare bill_text for clean_df by lowering all text and removing \n. Add first_date_read variable
    and truncate bill text so that language about passing and votes are removed.'''

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
        
    bill_text_df['bill_text'] = bill_text_df['bill_text'].apply(clean_bill_text)       
    bill_text_df['first_date_read'] = bill_text_df['bill_text'].apply(find_read_first_time_date)
    bill_text_df['bill_text'] = bill_text_df['bill_text'].apply(split_off_beginning_text)
    bill_text_df['bill_text'] = bill_text_df['bill_text'].apply(split_off_end_text)
    return bill_text_df



