#!/usr/bin/python

import math
import re

import pandas as pd
from doltpy.core.dolt import Dolt
from doltpy.etl import get_df_table_writer

normalize_chart: dict = {
    "BIDEN, JOSEPH R.": "JOSEPH BIDEN",
    "BLANKENSHIP, DON": "DON BLANKENSHIP",
    "JORGENSEN, JO": "JO JORGENSEN",
    "TRUMP, DONALD J.": "DONALD TRUMP",
    "None Of These Candidates": "EXPLICITLY ABSTAINED",
    "President and Vice President of the United States": "US PRESIDENT"
}

party_chart: dict = {
    "JOSEPH BIDEN": "DEMOCRATIC",
    "DON BLANKENSHIP": "CONSTITUTION",
    "JO JORGENSEN": "LIBERTARIAN",
    "DONALD TRUMP": "REPUBLICAN",
    "EXPLICITLY ABSTAINED": "NONE"
}

# Turns Out These Are The Counties
county_chart: dict = {
    'Carson City': 'CARSON CITY',
    'Churchill': 'CHURCHILL COUNTY',
    'Clark': 'CLARK COUNTY',
    'Douglas': 'DOUGLAS COUNTY',
    'Elko': 'ELKO COUNTY',
    'Esmeralda': 'ESMERALDA COUNTY',
    'Eureka': 'EUREKA COUNTY',
    'Humboldt': 'HUMBOLDT COUNTY',
    'Lander': 'LANDER COUNTY',
    'Lincoln': 'LINCOLN COUNTY',
    'Lyon': 'LYON COUNTY',
    'Mineral': 'MINERAL COUNTY',
    'Nye': 'NYE COUNTY',
    'Pershing': 'PERSHING COUNTY',
    'Storey': 'STOREY COUNTY',
    'Washoe': 'WASHOE COUNTY',
    'White Pine': 'WHITE PINE COUNTY'
}

repo = Dolt("working/us-president-precinct-results")
votes: pd.DataFrame = pd.read_csv('working/2020 General Election Precinct-Level Results.csv', skiprows=2,
                                  encoding="iso-8859-1")
# votes.drop(votes.tail(1).index, inplace=True)  # Drop Last Row
votes.dropna(inplace=True)  # Drop NaN Rows

# Lowercase Column Names
votes.columns = map(str.lower, votes.columns)

# Drop All Rows Not For Presidential Election
votes = votes[votes.contest.str.contains("President and Vice President of the United States")]

# Drop All Zero Votes and Ambiguous Votes
votes = votes[~(votes.votes == "0")]
votes = votes[~(votes.votes == "*")]

# Normalize Candidates
normalized: pd.Series = votes.selection.replace(normalize_chart, inplace=False)
votes.selection = normalized

# Jurisdiction, Precinct, Contest, Selection, Votes
# Election Year, Stage, Precinct, County, State, Jurisdiction, Candidate, Party, Writein, Office, Vote_Mode, Votes

# Rename Contest To Office and Normalize
votes.drop(columns="contest", inplace=True)
votes["office"] = "US PRESIDENT"

# Election Year 2020 - Stage gen - Writein 0 - State Nevada
votes["election_year"] = 2020
votes["stage"] = "gen"
votes["writein"] = 0
votes["state"] = "NEVADA"

# Vote Mode Total (Cause No Description Of Voting Methods)
votes["vote_mode"] = "Total"

# Rename Selection To candidate
votes.rename(index=str, columns={"selection": "candidate"}, inplace=True)

# Add Party Here
party: pd.Series = votes.candidate.replace(party_chart, inplace=False)
votes["party"] = party

# Add County Here
county: pd.Series = votes.jurisdiction.replace(county_chart, inplace=False)
votes["county"] = county

# Capitalize Columns
votes['jurisdiction'] = votes['jurisdiction'].str.upper()
votes['precinct'] = votes['precinct'].str.upper()

# Sadly not all precincts are mislabeled, otherwise I could just simply vectorize this
# votes['precinct'] = "PRECINCT " + votes['precinct'].values

# DEBUG: Drop Clark County
# votes = votes[~(votes.county == "CLARK COUNTY")]

normalize_precincts: list = []
for index, row in votes.iterrows():
    if row['precinct'] == "18-RANCHOS III-2":
        normalize_precincts.append("18-RANCHOS III")
    elif row['county'] == "WASHOE COUNTY" and re.sub("[^0-9]", "", row['precinct']) != "":
        normalize_precincts.append(re.sub("[^0-9]", "", row['precinct']))
    elif row['county'] == "CLARK COUNTY":
        normalize_precincts.append(row['precinct'])
    else:
        normalize_precincts.append(("PRECINCT " + row['precinct']) if row['precinct'].isnumeric() else row['precinct'])

votes.drop(columns="precinct", inplace=True)
votes["precinct"] = normalize_precincts
# votes.set_index("election_year", inplace=True)

print(votes)

columns: str = str(', '.join(votes.columns))
queries: list = []
for index, row in votes.iterrows():
    data: str = str(tuple(row.values))

    insert_query: str = f'''
        insert into vote_tallies ({columns}) values {data};
    '''
    queries.append(insert_query)

sql_file = open("working/us-president-precinct-results/sql-import-me-nevada.sql", mode="w")
sql_file.writelines(queries)

# votes.to_csv("working/us-president-precinct-results/import-me-nevada.csv")

# Prepare Data Writer
# raw_data_writer = get_df_table_writer("vote_tallies", lambda: votes,
#                                       ["election_year", "stage", "precinct", "county", "state", "jurisdiction",
#                                        "candidate", "vote_mode"])

# Write Data To Repo
# raw_data_writer(repo)

# dolt sql -q "insert into candidates (name, fec, fec_name) values (\"EXPLICITLY ABSTAINED\", null, null);"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"233 - CALIENTE NORTH\", \"LINCOLN COUNTY\", \"NEVADA\", \"LINCOLN\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"236 - CALIENTE SOUTH\", \"LINCOLN COUNTY\", \"NEVADA\", \"LINCOLN\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"PRECINCT 88\", \"HUMBOLDT COUNTY\", \"NEVADA\", \"HUMBOLDT\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"NEW RESIDENT\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"NO FIXED RES\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"PROVISIONAL\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"2062\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"2065\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"5047\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"7548\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"7592\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"7593\", \"WASHOE COUNTY\", \"NEVADA\", \"WASHOE\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"1711\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"1713\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"2399\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"2717\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3566\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3587\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3588\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3613\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3700\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3705\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3714\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"3753\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"5599\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"5649\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"5650\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"5655\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6467\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6468\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6484\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6600\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6717\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6718\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6740\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6750\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"6753\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"
# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"7596\", \"CLARK COUNTY\", \"NEVADA\", \"CLARK\");"