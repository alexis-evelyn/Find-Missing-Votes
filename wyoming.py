#!/usr/bin/python

import os
import re
from typing import List

import pandas as pd
from doltpy.core.dolt import Dolt

normalize_chart: dict = {
    "Joseph R. Biden and\nKamala D. Harris (D)": "JOSEPH BIDEN",
    "Jo Jorgensen and\nJeremy \"Spike\" Cohen (L)": "JO JORGENSEN",
    "Donald J. Trump and\nMichael R. Pence (R)": "DONALD TRUMP",
    "Brock Pierce and\nKarla Ballard (I)": "BROCK PIERCE",
    "Write-Ins": "WRITE-IN"
}

party_chart: dict = {
    "JOSEPH BIDEN": "DEMOCRATIC",
    "JO JORGENSEN": "LIBERTARIAN",
    "DONALD TRUMP": "REPUBLICAN",
    "BROCK PIERCE": "NEW YORK INDEPENDENCE PARTY",
    "WRITE-IN": "None"
}

repo = Dolt("working/us-president-precinct-results")
sql_file = open("working/us-president-precinct-results/sql-import-me-wyoming.sql", mode="w")

wyoming_precincts: List[str] = []
with open("./working/us-president-precinct-results/wyoming-precincts.csv", 'r') as f:
    for line in f:
        # print(line, end='')
        wyoming_precincts.append(line)

for file in os.listdir("working/Wyoming_General_CSV/"):
    if "County" not in file:
        continue

    votes: pd.DataFrame = pd.read_csv(f'working/Wyoming_General_CSV/{file}', skiprows=1,
                                      encoding="iso-8859-1")

    votes.dropna(inplace=True)  # Drop NaN Rows
    votes.drop(votes.tail(1).index, inplace=True)  # Drop Last Row (Totals Row)

    # Drop All Columns After This Column
    votes = votes.loc[:, :'Write-Ins']

    # Normalize Candidate Names
    # candidates: pd.Series = votes.jurisdiction.replace(normalize_chart, inplace=False)
    # votes["candidates"] = candidates

    fixed_votes: pd.DataFrame = pd.DataFrame(columns=["election_year", "stage", "precinct", "county", "state",
                                                      "jurisdiction", "candidate", "party", "writein", "office",
                                                      "vote_mode", "votes"])
    for _, row in votes.iterrows():
        for index in range(1, 7):
            if votes.columns[index-1] not in normalize_chart.keys():
                continue

            # print(normalize_chart[votes.columns[index-1]])
            precinct: str = ""
            for wyoming_precinct in wyoming_precincts:
                matches: list = re.findall("(\D){}\"*,(.*){}".format(row['Precinct'], file.split('_')[1].upper()), wyoming_precinct)
                for match in matches:
                    # if row['Precinct'] == "2-1" or row['Precinct'] == "2-10":
                    #     print("{}\"*,(.*){} and {}".format(row['Precinct'], file.split('_')[1].upper(), wyoming_precinct))

                    precinct_split: list = wyoming_precinct.strip("\n").split(",")
                    precinct = ",".join(precinct_split[:len(precinct_split)-2])

                    if '"' in precinct:
                        precinct = precinct.rstrip('"').lstrip('"')

                    # if row['Precinct'] == "2-1" or row['Precinct'] == "2-10":
                    #     print(f"Precinct: {precinct}")

                    break

            # if "FREMONT" in file.split('_')[1].upper():
            #     continue

            county: str = "{} {}".format(file.split('_')[1].upper(), file.split('_')[2].upper())
            jurisdiction: str = file.split('_')[1].upper()

            if county == "BIG HORN":
                county = "BIG HORN COUNTY"
                jurisdiction = "BIG HORN"

            if county == "HOT SPRINGS":
                county = "HOT SPRINGS COUNTY"
                jurisdiction = "HOT SPRINGS"

            if precinct == '':
                precinct = row['Precinct']
                insert_query: str = f'''
                    insert into precincts (precinct, county, state, jurisdiction) values ({precinct}, {county}, "WYOMING", {jurisdiction});
                '''

                sql_file.writelines(insert_query)

            # Election Year 2020 - Stage gen - Writein 0 - State Nevada
            d_row: dict = {
                "election_year": 2020,
                "stage": "gen",
                "writein": 1 if normalize_chart[votes.columns[index-1]] == "WRITE-IN" else 0,
                "state": "WYOMING",
                "vote_mode": "Total",
                "office": "US PRESIDENT",
                "precinct": precinct.upper(),
                "county": county,
                "jurisdiction": jurisdiction,
                "candidate": normalize_chart[votes.columns[index-1]],
                "party": party_chart[normalize_chart[votes.columns[index-1]]],
                "votes": row[votes.columns[index-1]]
            }

            fixed_votes = fixed_votes.append(d_row, ignore_index=True)

    # print(f"File: {file}")
    # print("-------------------------------------------------------------")
    # print(fixed_votes)
    # print("-------------------------------------------------------------")

    # Drop 0 Votes
    fixed_votes = fixed_votes[~(fixed_votes.votes == "0.0")]

    columns: str = str(', '.join(fixed_votes.columns))
    queries: list = []
    for index, row in fixed_votes.iterrows():
        data: str = str(tuple(row.values))

        insert_query: str = f'''
            insert into vote_tallies ({columns}) values {data};
        '''
        queries.append(insert_query)

    sql_file.writelines(queries)

# dolt sql -q "insert into precincts (precinct, county, state, jurisdiction) values (\"1-27\", \"CAMPBELL COUNTY\", \"WYOMING\", \"CAMPBELL\");"