
import pandas as pd

'''
The table below was created manually using the report "State of Washington Members of the Legislature
1889-2018". The report can be found here: http://leg.wa.gov/History/Legislative/Documents/MembersOfLeg2018.pdf.

Running this file will populate party_minority_history.csv.
'''

party_minority_history = [{'biennium': '1991-92', 'agency': 'Senate', 'minority_party': 'D'},
            {'biennium': '1991-92', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '1993-94', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '1993-94', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '1995-96', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '1995-96', 'agency': 'House', 'minority_party': 'D'},

            {'biennium': '1997-98', 'agency': 'Senate', 'minority_party': 'D'},
            {'biennium': '1997-98', 'agency': 'House', 'minority_party': 'D'},

            {'biennium': '1999-00', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '1999-00', 'agency': 'House', 'minority_party': 'Even'},

            {'biennium': '2001-02', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2001-02', 'agency': 'House', 'minority_party': 'Even'},

            {'biennium': '2003-04', 'agency': 'Senate', 'minority_party': 'D'},
            {'biennium': '2003-04', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2005-06', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2005-06', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2007-08', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2007-08', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2009-10', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2009-10', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2011-12', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2011-12', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2013-14', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2013-14', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2015-16', 'agency': 'Senate', 'minority_party': 'D'},
            {'biennium': '2015-16', 'agency': 'House', 'minority_party': 'R'},

            {'biennium': '2017-18', 'agency': 'Senate', 'minority_party': 'R'},
            {'biennium': '2017-18', 'agency': 'House', 'minority_party': 'R'},
]

party_minority_history_df = pd.DataFrame(party_minority_history)
party_minority_history_df.to_csv('party_minority_history.csv', sep="|", index=False)