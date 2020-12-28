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

# Figure Out What Counties
votes["county"] = None

# Vote Mode Total (Cause No Description Of Voting Methods)
votes["vote_mode"] = "Total"

# Rename Selection To candidate
votes.rename(index=str, columns={"Selection": "candidate"}, inplace=True)

print(votes)
