import psycopg2 as pg2
import pandas as pd
import sqlalchemy
import functools
from web_scrape_functions import scrape_bill_topic_table, scrape_bill_url
from WA_state_API_functions import get_bill_data, get_sponsor_data, get_committee_data, get_committee_member_data, get_and_reorganize_rollcall_data

# Create connection to wa_leg_raw postgres database. Example below.
    # engine = sqlalchemy.create_engine('postgresql://emilykarboski@localhost:5432/wa_leg_raw')
    # con = engine.connect()


def save_bill_data_STEP_ONE():
    """Fetches bill data from the API, using get_bill_data and all bienniums between 1991-92 to 2017-18.
    API data is put into pandas data frames and then pushed to the postgreSQL database wa_leg_raw.

    Creates table: bill_api
    """
    bienniums = ['1991-92', '1993-94', '1995-96', '1997-98', '1999-00',
                 '2001-02', '2003-04', '2005-06', '2007-08', '2009-10', 
                 '2011-12', '2013-14', '2015-16', '2017-18']

    bill_dfs = [get_bill_data(b, 'Bills') for b in bienniums]
    for df in bill_dfs:
        df.to_sql('bill_api', con, if_exists='append', index=False)



def save_sponsor_data_STEP_TWO():
    """Fetches sponsor data from the API, using get_sponsor_data. All pairs of bill_id and
    biennium (loaded from bill_api) are used to fetch sponsor data on all available bills.
    Data is then pushed to the postgreSQL database wa_leg_raw.

    Creates table: sponsor_api
    """

    bill_df = pd.read_sql_query('select * from "bill_api"',con=engine)

    sponsor_dfs = []
    for bill_id, biennium in zip(bill_df['bill_id'], 
                                 bill_df['biennium']):
        try: 
            sponsor_df = get_sponsor_data(biennium, bill_id)
            sponsor_df['biennium'] = biennium
            sponsor_df['bill_id'] = bill_id
            sponsor_dfs.append(sponsor_df)
        except:
            continue
    
    for df in sponsor_dfs:
        df.to_sql('sponsor_api', con, if_exists='append', index=False)



def save_committee_data_STEP_THREE():
    """Fetches committee data from the API, using get_committee_data and all bienniums
    between 1991-92 to 2017-28. The committee data is used to fetch committee member data.
    Data is then pushed to the postgreSQL database wa_leg_raw.

    Creates two tables: committee_api, committee_member_api
    """
    # Retrieve API committee data for all bienniums and put in postgres table 'committee_api'
    bienniums = ['1991-92', '1993-94', '1995-96', '1997-98', '1999-00',
                 '2001-02', '2003-04', '2005-06', '2007-08', '2009-10', 
                 '2011-12', '2013-14', '2015-16', '2017-18']

    committee_dfs = [get_committee_data(b) for b in bienniums]
    committee_data_all_bienniums = functools.reduce((lambda x, y: x.append(y)), committee_dfs)
    committee_data_all_bienniums.to_sql('committee_api', con, if_exists='replace', index=False)
    
    # Retrieve API committee member data and put in postgres table 'committee_member_api'
    com_member_dfs = []
    for biennium, agency, name in zip(committee_data_all_bienniums['biennium'], 
                                committee_data_all_bienniums['agency'], 
                                committee_data_all_bienniums['name']):
        try: 
            com_member_df = get_committee_member_data(biennium, agency, name)
            com_member_dfs.append(com_member_df)
        except:
            continue

    for df in com_member_dfs:
        df.to_sql('committee_member_api', con, if_exists='append', index=False)



def save_vote_data_STEP_FOUR():
    '''Fetches vote (rollcall) data from the API, using get_and_reorganize_rollcall_data. All pairs of bill_num and
    biennium (loaded from bill_api) are used to fetch vote data on all available bills.
    Data is then pushed to the postgreSQL database wa_leg_raw.

    Creates table: vote_api
    '''  

    # Load bill data from newly created postgres table
    bill_df = pd.read_sql_query('select * from "bill_api"',con=engine)

    # Isolate bill numbers and create dataframe with unique biennium and bill_nums
    bill_df['bill_num'] = bill_df['bill_id'].str[-4:]
    bill_nums_and_bienniums = bill_df.loc[:, ['biennium', 'bill_num']]
    unique_bill_nums_and_bienniums = bill_nums_and_bienniums.drop_duplicates(keep='first')

    # Retrieve API voting data and put in postgres table
    vote_dfs = []
    for biennium, bill_num in zip(unique_bill_nums_and_bienniums['biennium'], 
                                unique_bill_nums_and_bienniums['bill_num']):
        try: 
            vote_df = get_and_reorganize_rollcall_data(biennium, bill_num)
            vote_dfs.append(vote_df)
        except:
            continue

    for df in vote_dfs:
        df.to_sql('vote_api', con, if_exists='append', index=False)



def save_topic_data_STEP_FIVE():
    '''Scrapes topic data, using scrape_bill_topic_table and all years between 1991 and 2018. 
    Data is then pushed to the postgreSQL database wa_leg_raw.

    Creates table: topic_scrape
    '''  
    years = ['1991', '1992', '1993', '1994', '1995', '1996', '1997', '1998', '1999', '2000'
             '2001', '2002', '2003', '2004', '2005', '2006', '2007', '2008', '2009', '2010', 
             '2011', '2012', '2013', '2014', '2015', '2016', '2017', '2018']

    topic_dfs = []
    for year in years:
        topic_df = scrape_bill_topic_table(year)
        topic_df['year'] = year
        topic_dfs.append(topic_df)

    for df in topic_dfs:
        df.to_sql('topic_scrape', con, if_exists='append', index=False)
        
        

# This function has been depricated in favor of create_staging_bill_text_df_STEP_FOUR in 
# create_staging_tables.py. The function below scraped all possible bills, where as the new
# function only scrapes the bills with available voting data (cutting from 100,000 to 19,000).

# def save_bill_text_data_STEP_SIX():
#     '''Scrapes bill text for each url provided from bill_api, using scrape_bill_url. 
#     Data is then pushed to the postgreSQL database wa_leg_raw.
    
#     Table created: bill_text_scrape
#     '''     
#     # Load bill data from bill_api postgres table
#     bill_df = pd.read_sql_query('select * from "bill_api"',con=engine)

#     # Scrape bill text and put in postgres table
#     bill_text_matrix = []
#     for bill_id, biennium, url in zip(bill_df['bill_id'], bill_df['biennium'], bill_df['htm_url']):
        
#         try: 
#             row = []
#             bill_text = scrape_bill_url(url)
#             row.append(bill_id)
#             row.append(biennium)
#             row.append(bill_text)
#             bill_text_matrix.append(row)
#         except:
#             continue
        
#     bill_text_df = pd.DataFrame(bill_text_matrix)
#     bill_text_df.columns = ['bill_id', 'biennium', 'bill_text']
    
#     bill_text_df.to_sql('bill_text_api', con, if_exists='append', index=False)




    