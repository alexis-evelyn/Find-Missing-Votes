#!/usr/bin/python

import pandas as pd
import math

# Party Labels
# DEM = Democratic
# REP = Republican
# AI = American Independent
# GRN = Green
# LIB = Libertarian
# PF = Peace and Freedom

# Candidate Labels
# Joseph Biden - Democratic
# Donald Trump - Republican
# Jo Jorgensen - Libertarian
# Don Blankenship - Constitution Party
# Roque De La Fuente Guerra - American Independent
# Howie Hawkins - Green
# Gloria La Riva - Peace and Freedom
# Brian Carroll - American Solidarity Party
# Mark Charles - Independent
# Joseph Kishore - Socialist Equality Party
# Brock Pierce - Life, Liberty, and the pursuit of Happiness
# Jesse Ventura - Green
# Other Candidates - WRITE-IN
from doltpy.core import Dolt

candidates: dict = {
    "Joseph Biden": {"id": 1, "party": 2},
    "Donald Trump": {"id": 2, "party": 3},
    "Jo Jorgensen": {"id": 3, "party": 4},
    "Don Blankenship": {"id": 4, "party": 5},
    "Roque De La Fuente Guerra": {"id": 6, "party": 6},
    "Howie Hawkins": {"id": 7, "party": 7},
    "Gloria La Riva": {"id": 8, "party": 8},
    "Brian Carroll": {"id": 9, "party": 9},
    "Mark Charles": {"id": 10, "party": 10},
    "Joseph Kishore": {"id": 11, "party": 11},
    "Brock Pierce": {"id": 12, "party": 12},
    "Jesse Ventura": {"id": 13, "party": 7},
    "Other": {"id": 5, "party": 1},
}

file: str = "./california-18-presidential.csv"
# file: str = "./nevada.csv"
state: str = "California"
repo: Dolt = Dolt("./county-level-votes")


def add_vote(county: str, candidate: str, votes: int):
    # print(f"County: {county} - Candidate: {candidate} - Votes: {votes}")

    insert_vote_query = '''
        insert into votes (county, candidate, votes, party) values ({county}, {candidate}, {votes}, {party})
    '''.format(county=0, candidate=0, votes=votes, party=0)

    repo.sql(insert_vote_query)


votes: pd.DataFrame = pd.read_csv(file)

for index, row in votes.iterrows():
    for candidate in votes.keys()[1:]:
        county: str = row[0]

        vote_float: float = float(str(row[candidate]).replace(',', ''))

        if math.isnan(vote_float):
            print(f"Skipping County: {row[0]} - Candidate: {candidate} Because Count is NaN")
            continue

        count: int = int(math.floor(vote_float))

        if count == 0:
            print(f"Skipping County: {row[0]} - Candidate: {candidate} Because Count is {count}")
            continue

        add_vote(county=county, candidate=candidate, votes=count)