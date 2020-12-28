

import pandas as pd

normalize_chart: dict = {
    "BIDEN, JOSEPH R.": "JOSEPH BIDEN",
    "BLANKENSHIP, DON": "DON BLANKENSHIP",
    "JORGENSEN, JO": "JO JORGENSEN",
    "TRUMP, DONALD J.": "DONALD TRUMP",
    "None Of These Candidates": "ABSTAINED"
}

votes: pd.DataFrame = pd.read_csv('working/2020 General Election Precinct-Level Results.csv', skiprows=2, encoding="iso-8859-1")
# votes.drop(votes.tail(1).index, inplace=True)  # Drop Last Row
votes.dropna(inplace=True)  # Drop NaN Rows

# Drop All Rows Not For Presidential Election
votes = votes[votes.Contest.str.contains("President and Vice President of the United States")]

# ['BIDEN, JOSEPH R.' 'BLANKENSHIP, DON' 'JORGENSEN, JO', 'None Of These Candidates' 'TRUMP, DONALD J.']
normalized: pd.Series = votes.Selection.replace(normalize_chart, inplace=False)
votes.Selection = normalized

print(votes)
