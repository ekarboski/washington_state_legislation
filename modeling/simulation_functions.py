import pandas as pd
import numpy as np
import string
import random


def make_predicted_votes_dataframes(label_df, bill_id):
    """Make two pandas dataframes (house and senate) by selecting a bill from label_df.
    
    Args:
        label_df: a pandas dataframe loaded from label_pickle.pkl
        bill_id (str): a bill id such as "HB 1727"
    
    
    Returns:
        predicted_votes_house: a pandas series with vote probabilities for the house
        predicted_votes_senate: a pandas series with vote probabilities for the senate
        
    """
    predicted_votes_house = label_df.loc[(label_df['bill_id'] == bill_id) & (label_df['voting_agency'] == 0), 'predicted_vote']
    predicted_votes_senate = label_df.loc[(label_df['bill_id'] == bill_id) & (label_df['voting_agency'] == 1), 'predicted_vote']
    return predicted_votes_house, predicted_votes_senate


def run_single_vote_simulation(predicted_votes):
    """Runs a simulation of voting on a bill. A random number is generated for each legislator 
    and if the random number is below the legislator's predidcted probability of voting yea, then the 
    vote is considered a yea. All votes are aggregated and if more than 50% of the votes are yea, the bill
    is considered passed.
    
     Args:
        predicted_votes: a pandas series with vote probabilities
        
    Returns:
        (int) 1 if bill passes, 0 if bill does not pass
    """
    votes = []
    for vote in predicted_votes:
        x = random.random()
        if vote >= x:
            votes.append(1)
        else:
            votes.append(0)
    if (sum(votes) / len(votes)) >= 0.5:
        return 1
    else:
        return 0


def compile_vote_simulations(predicted_votes, n):
    """Run n simulations of legislators voting and return the proportion of time that the bill passed.
    
    Args:
        predicted_votes: a pandas series with vote probabilities
        n (int): number of simulations to run
        
    Returns:
        (int) proportion of simulations in which the bill passed
    
    """
    sim_results = []
    for _ in range(n):
        result = run_single_vote_simulation(predicted_votes)
        sim_results.append(result)
    return sum(sim_results) / len(sim_results)


def find_prob_of_bill_passing(label_df, bill_id, n=10000):
    """Using the probabilities generated from compile_vote_simulations, return the probability that 
    the bill will pass in both agencies.
    
    Args:
        label_df: a pandas dataframe loaded from label_pickle.pkl
        bill_id (str): a bill id such as "HB 1727"
        
    Returns:
        overall_prob (int): probability that the bill will pass in both agencies
        house_prob (int): proportion of simulations in which the bill passed in the house
        senate_prob (int): proportion of simulations in which the bill passed in the senate
    """
    predicted_votes_house, predicted_votes_senate = make_predicted_votes_dataframes(label_df, bill_id)

    house_prob = compile_vote_simulations(predicted_votes_house, n)
    senate_prob = compile_vote_simulations(predicted_votes_senate, n)
    overall_prob = house_prob * senate_prob

    return overall_prob, house_prob, senate_prob


def find_prob_of_passing_all_bills(label_df):
    """Find the probability that a bill will pass in each agency based on the predicted probabilities of each legislator's vote.
    
    Args:
        label_df: a pandas dataframe loaded from label_pickle.pkl
        
    Returns:
        all_bills_prob_passing_df: pandas dataframe with four columns: bill_id, overall_prob, house_prob and senate_prob
    
    """
    all_bill_probs = []
    for bill_id in label_df['bill_id'].unique():
        overall_prob, house_prob, senate_prob = find_prob_of_bill_passing(label_df, bill_id)
        row = [bill_id, overall_prob, house_prob, senate_prob]
        all_bill_probs.append(row)
    all_bills_prob_passing_df = pd.DataFrame(all_bill_probs, columns=['bill_id', 'overall_prob', 'house_prob', 'senate_prob'])
    return all_bills_prob_passing_df

