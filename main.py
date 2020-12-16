import logging

from doltpy.core import Dolt, system_helpers
import pandas as pd
from doltpy.etl import get_df_table_writer

# Verify Missing Data
# select * from vote_tallies where election_year=2016 and county="AURORA COUNTY" and jurisdiction="AURORA" and state="SOUTH DAKOTA";

# This is to get DoltPy's Logger To Shut Up When Running `this_script.py -h`
logging.Logger.setLevel(system_helpers.logger, logging.CRITICAL)

repo: Dolt = Dolt("./us-president-precinct-results")

# Added Counties/States/Jurisdictions
# select county, state, jurisdiction from vote_tallies where election_year=2020 group by county, state, jurisdiction;

# All Known Counties/States/Jurisdictions
# select county, state, jurisdiction from precincts group by county, state, jurisdiction;

# Year In Question
year: int = 2016

# Added Counties/States/Jurisdictions
added_votes_query = f"""
    select county, state, jurisdiction from vote_tallies where election_year={year} group by county, state, jurisdiction;
"""

# All Known Counties/States/Jurisdictions
total_votes_query = f"""
    select county, state, jurisdiction from precincts group by county, state, jurisdiction;
"""

added_votes: dict = repo.sql(added_votes_query, result_format='json')["rows"]
total_votes: dict = repo.sql(total_votes_query, result_format='json')["rows"]

added_votes_df: pd.DataFrame = pd.DataFrame(added_votes)
total_votes_df: pd.DataFrame = pd.DataFrame(total_votes)

# print(total_votes_df)

all_votes: pd.DataFrame = pd.merge(total_votes_df, added_votes_df, how='outer', suffixes=('','_y'), indicator=True)
missing_votes_df: pd.DataFrame = all_votes[all_votes['_merge'] == 'left_only'][total_votes_df.columns]

# print(missing_votes_df)

missing_votes_df.to_csv(f"./{year}-missing-votes.csv")