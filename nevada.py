#!/usr/bin/python

import pandas as pd
from doltpy.core.dolt import Dolt

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
    'Esmeralda': 'ESMERELDA COUNTY',
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
votes: pd.DataFrame = pd.read_csv('working/2020 General Election Precinct-Level Results.csv', skiprows=2, encoding="iso-8859-1")
# votes.drop(votes.tail(1).index, inplace=True)  # Drop Last Row
votes.dropna(inplace=True)  # Drop NaN Rows

# Drop All Rows Not For Presidential Election
votes = votes[votes.Contest.str.contains("President and Vice President of the United States")]

# Normalize Candidates
normalized: pd.Series = votes.Selection.replace(normalize_chart, inplace=False)
votes.Selection = normalized

# Jurisdiction, Precinct, Contest, Selection, Votes
# Election Year, Stage, Precinct, County, State, Jurisdiction, Candidate, Party, Writein, Office, Vote_Mode, Votes
# TODO: County, Party

# Rename Contest To Office and Normalize
votes.drop(columns="Contest", inplace=True)
votes["office"] = "US PRESIDENT"

# Election Year 2020 - Stage gen - Writein 0 - State Nevada
votes["election_year"] = 2020
votes["stage"] = "gen"
votes["writein"] = 0
votes["state"] = "NEVADA"

# Vote Mode Total (Cause No Description Of Voting Methods)
votes["vote_mode"] = "Total"

# Rename Selection To candidate
votes.rename(index=str, columns={"Selection": "candidate"}, inplace=True)

# Add Party Here
party: pd.Series = votes.candidate.replace(party_chart, inplace=False)
votes["party"] = party

# Add County Here
county: pd.Series = votes.Jurisdiction.replace(county_chart, inplace=False)
votes["county"] = county

# Capitalize Columns
votes['Jurisdiction'] = votes['Jurisdiction'].str.upper()
votes['Precinct'] = votes['Precinct'].str.upper()

# Lowercase Column Names
votes.columns = map(str.lower, votes.columns)

print(votes)
# print(votes.Jurisdiction.unique())
