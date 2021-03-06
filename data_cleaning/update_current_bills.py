import requests
import xml.etree.ElementTree as ET
import pandas as pd

import psycopg2 as pg2
import sqlalchemy

from data_aquisition.WA_state_API_functions import get_status_data

def save_status_data():
    '''This data will be collected to identify which bills have not yet been voted on.

    Creates table: status_api
    '''  
    # Create connection to wa_leg_raw postgres database
    engine = sqlalchemy.create_engine('postgresql://emilykarboski@localhost:5432/wa_leg_raw')
    con = engine.connect()

    # Load bill data from newly created postgres table
    bill_df = pd.read_sql_query('select * from "bill_api"',con=engine)

    # Isolate bill numbers and create dataframe with unique biennium and bill_nums
    bill_df['bill_num'] = bill_df['bill_id'].str[-4:]
    bill_nums_and_bienniums = bill_df.loc[:, ['biennium', 'bill_num']]
    unique_bill_nums_and_bienniums = bill_nums_and_bienniums.drop_duplicates(keep='first')
    unique_2017 = unique_bill_nums_and_bienniums[unique_bill_nums_and_bienniums['biennium'] == '2017-18']

    # Retrieve API status data
    status_dcts = []
    for biennium, bill_num in zip(unique_2017['biennium'], 
                                unique_2017['bill_num']):
        try: 
            status_dct = get_status_data(biennium, bill_num)
            status_dcts.append(status_dct)
        except:
            continue
    status_df = pd.DataFrame(status_dcts)
    
    # Filter out bills that have passed or failed
    def in_process(history_line):
        """The history_line has indicators about whether the bill has been voted on. This functions uses
        those indicators to filter out bills that are no longer in process.
        
        Args: 
            history_line: feature of status_df
        
        Returns: 0 if history line contains indicator (bill has been voted on)
                 1 if history line contains indicator (bill has not been voted on)
        """
        history_line = history_line.lower()
        done_indications = ['adopted', 'effective date', 'failed', 'filed with secretary of state', 
                            'vetoed', 'chapter']
        for ind in done_indications:
            if ind in history_line:
                return 0
        return 1
    
    status_df['bill_in_process'] = status_df['history_line'].apply(in_process)
    bills_in_process = status_df[status_df['bill_in_process'] == 1]
    
    # Put in postgres table
    bills_in_process.to_sql('status_api', con, if_exists='replace', index=False)