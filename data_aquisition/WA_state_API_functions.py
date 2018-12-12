import requests
import xml.etree.ElementTree as ET
import pandas as pd

def get_committee_data(biennium):
    '''Input
       biennium: str, for example '2015-16'
       '''

    r = requests.get('http://wslwebservices.leg.wa.gov/CommitteeService.asmx/GetCommittees?biennium={}'.format(biennium))
    r.text
    root = ET.fromstring(r.text)

    committee_info = []

    for committee in root.findall('{http://WSLWebServices.leg.wa.gov/}Committee'):
        committee_dct = {}
        committee_dct['name'] = committee.find('{http://WSLWebServices.leg.wa.gov/}Name').text
        committee_dct['id'] = committee.find('{http://WSLWebServices.leg.wa.gov/}Id').text
        committee_dct['long_name'] = committee.find('{http://WSLWebServices.leg.wa.gov/}LongName').text
        committee_dct['agency'] = committee.find('{http://WSLWebServices.leg.wa.gov/}Agency').text
        committee_dct['acronym'] = committee.find('{http://WSLWebServices.leg.wa.gov/}Acronym').text
        committee_dct['biennium'] = biennium
        committee_info.append(committee_dct)

    committee_data = pd.DataFrame(committee_info)
    return committee_data


def get_committee_member_data(biennium, agency, committee_name):
    '''Input
       biennium: str, for example '2015-16'
       agency: str, 'Senate' or 'House
       commmittee_name: str, for example 'Education'
       '''
    
    r = requests.get('http://wslwebservices.leg.wa.gov/CommitteeService.asmx/GetCommitteeMembers?biennium={}&agency={}&committeeName={}'.format(biennium, agency, committee_name))
    r.text
    root = ET.fromstring(r.text)

    all_dcts = []

    for child in root.findall('{http://WSLWebServices.leg.wa.gov/}Member'):
        dct = {}
        dct['name'] = child.find('{http://WSLWebServices.leg.wa.gov/}Name').text
        dct['id'] = child.find('{http://WSLWebServices.leg.wa.gov/}Id').text
        dct['long_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}LongName').text
        dct['agency'] = child.find('{http://WSLWebServices.leg.wa.gov/}Agency').text
        dct['acronym'] = child.find('{http://WSLWebServices.leg.wa.gov/}Acronym').text
        dct['party'] = child.find('{http://WSLWebServices.leg.wa.gov/}Party').text
        dct['district'] = child.find('{http://WSLWebServices.leg.wa.gov/}District').text
        dct['first_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}FirstName').text
        dct['last_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}LastName').text
        dct['biennium'] = biennium
        dct['committee_name'] = committee_name
        all_dcts.append(dct)

    committee_data = pd.DataFrame(all_dcts)
    return committee_data


def get_bill_data(biennium, documentClass):
    '''Input
       biennium: str, for example '2015-16'
       documentClass: str, for example 'Bills'
       '''
    
    r = requests.get('http://wslwebservices.leg.wa.gov/LegislativeDocumentService.asmx/GetAllDocumentsByClass?biennium={}&documentClass={}'.format(biennium, documentClass))
    r.text
    root = ET.fromstring(r.text)
    
    all_dcts = []

    for child in root.findall('{http://WSLWebServices.leg.wa.gov/}LegislativeDocument'):
        dct = {}
        dct['name'] = child.find('{http://WSLWebServices.leg.wa.gov/}Name').text
        dct['biennium'] = child.find('{http://WSLWebServices.leg.wa.gov/}Biennium').text
        dct['long_friendly_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}LongFriendlyName').text
        dct['description'] = child.find('{http://WSLWebServices.leg.wa.gov/}Description').text
        dct['type'] = child.find('{http://WSLWebServices.leg.wa.gov/}Type').text
        dct['class'] = child.find('{http://WSLWebServices.leg.wa.gov/}Class').text
        dct['htm_url'] = child.find('{http://WSLWebServices.leg.wa.gov/}HtmUrl').text
        dct['htm_last_modified_date'] = child.find('{http://WSLWebServices.leg.wa.gov/}HtmLastModifiedDate').text
        dct['htm_create_date'] = child.find('{http://WSLWebServices.leg.wa.gov/}HtmCreateDate').text
        dct['bill_id'] = child.find('{http://WSLWebServices.leg.wa.gov/}BillId').text
        all_dcts.append(dct)

    bill_data = pd.DataFrame(all_dcts)
    return bill_data


def get_rollcall_data(biennium, billNumber):
    '''Input
       biennium: str, for example '2015-16'
       billNumber: str, for example '1003'
       '''

    r = requests.get('http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetRollCalls?biennium={}&billNumber={}'.format(biennium, billNumber))
    r.text
    root = ET.fromstring(r.text)
    
    all_dcts = []

    for child in root.findall('{http://WSLWebServices.leg.wa.gov/}RollCall'):
        dct = {}
        dct['voting_agency'] = child.find('{http://WSLWebServices.leg.wa.gov/}Agency').text
        dct['bill_id'] = child.find('{http://WSLWebServices.leg.wa.gov/}BillId').text
        dct['biennium'] = child.find('{http://WSLWebServices.leg.wa.gov/}Biennium').text
        dct['motion'] = child.find('{http://WSLWebServices.leg.wa.gov/}Motion').text
        dct['sequence_number'] = child.find('{http://WSLWebServices.leg.wa.gov/}SequenceNumber').text
        dct['vote_date'] = child.find('{http://WSLWebServices.leg.wa.gov/}VoteDate').text
        
        for grandchild in child.findall('{http://WSLWebServices.leg.wa.gov/}YeaVotes'):
            dct['yea_vote_count'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}Count').text
            dct['members_voting_yea'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}MembersVoting').text
            
        for grandchild in child.findall('{http://WSLWebServices.leg.wa.gov/}NayVotes'):
            dct['nay_vote_count'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}Count').text
            dct['members_voting_nay'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}MembersVoting').text
            
        for grandchild in child.findall('{http://WSLWebServices.leg.wa.gov/}AbsentVotes'):
            dct['absent_vote_count'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}Count').text
            dct['members_voting_absent'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}MembersVoting').text
            
        for grandchild in child.findall('{http://WSLWebServices.leg.wa.gov/}ExcusedVotes'):
            dct['excused_vote_count'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}Count').text
            dct['members_voting_excused'] = grandchild.find('{http://WSLWebServices.leg.wa.gov/}MembersVoting').text
            
        votes = []    
        for grandchild in child.findall('{http://WSLWebServices.leg.wa.gov/}Votes'):
            for greatgrandchild in grandchild.findall('{http://WSLWebServices.leg.wa.gov/}Vote'):
                votes_dct = {}
                votes_dct['legislator_id'] = greatgrandchild.find('{http://WSLWebServices.leg.wa.gov/}MemberId').text
                votes_dct['legislator_name'] = greatgrandchild.find('{http://WSLWebServices.leg.wa.gov/}Name').text
                votes_dct['legislator_vote'] = greatgrandchild.find('{http://WSLWebServices.leg.wa.gov/}VOte').text
                votes.append(votes_dct)
        dct['votes'] = votes
        all_dcts.append(dct)

    rolecall_data = pd.DataFrame(all_dcts)
    return rolecall_data


def get_sponsor_data(biennium, billId):
    '''Input
       biennium: str, for example '2015-16'
       billId: str, for example 'HB 1003'
       '''
    
    r = requests.get('http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetSponsors?biennium={}&billId=string{}'.format(biennium, billId))
    r.text
    root = ET.fromstring(r.text)
    
    all_dcts = []

    for child in root.findall('{http://WSLWebServices.leg.wa.gov/}Sponsor'):
        dct = {}
        dct['sponsor_id'] = child.find('{http://WSLWebServices.leg.wa.gov/}Id').text
        dct['sponsor_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}Name').text
        dct['sponsor_long_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}LongName').text
        dct['sponsor_agency'] = child.find('{http://WSLWebServices.leg.wa.gov/}Agency').text
        dct['sponsor_type'] = child.find('{http://WSLWebServices.leg.wa.gov/}Type').text
        dct['sponsor_order'] = child.find('{http://WSLWebServices.leg.wa.gov/}Order').text
        dct['sponsor_first_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}FirstName').text
        dct['sponsor_last_name'] = child.find('{http://WSLWebServices.leg.wa.gov/}LastName').text
        all_dcts.append(dct)

    sponsor_data = pd.DataFrame(all_dcts)
    return sponsor_data

        
def get_and_reorganize_rollcall_data(biennium, billNumber):
    '''Retrieves roll call data and organizes it so that each voter is given it's own line.
    
    Input
       biennium: str, for example '2015-16'
       billNumber: str, for example '1003'
       '''

    rollcall_df = get_rollcall_data(biennium, billNumber)

    final = []
    for i, votes in enumerate(rollcall_df['votes']):
        for votes_dct in votes:
            new_row = rollcall_df.iloc[i][['voting_agency', 'biennium', 'bill_id', 'motion', 
                                                  'sequence_number', 'vote_date']].to_dict()
            new_row['voter_id'] = votes_dct['legislator_id']
            new_row['voter_name'] = votes_dct['legislator_name']
            new_row['vote'] = votes_dct['legislator_vote']
            final.append(new_row)

    return pd.DataFrame(final)


def get_status_data(biennium, billNumber):
    '''Input
       biennium: str, for example '2015-16'
       billNumber: str, for example '1003'
       '''

    r = requests.get('http://wslwebservices.leg.wa.gov/LegislationService.asmx/GetCurrentStatus?biennium={}&billNumber={}'.format(biennium, billNumber))
    r.text
    root = ET.fromstring(r.text)
    
    dct = {}
    dct['status'] = root.find('{http://WSLWebServices.leg.wa.gov/}Status').text
    dct['bill_id'] = root.find('{http://WSLWebServices.leg.wa.gov/}BillId').text
    dct['history_line'] = root.find('{http://WSLWebServices.leg.wa.gov/}HistoryLine').text
    dct['action_date'] = root.find('{http://WSLWebServices.leg.wa.gov/}ActionDate').text
    dct['status'] = root.find('{http://WSLWebServices.leg.wa.gov/}Status').text
    dct['biennium'] = biennium
    dct['bill_num'] = billNumber

    return dct
        