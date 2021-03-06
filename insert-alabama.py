import ast
import json
import logging
import os
import csv
import math
from typing import TextIO

from doltpy.core import system_helpers, Dolt, DoltException
import pandas as pd

# This is to get DoltPy's Logger To Shut Up When Running `this_script.py -h`
logging.Logger.setLevel(system_helpers.logger, logging.DEBUG)

# /Users/alexis/IdeaProjects/FindMissingVotes/skip/alabama-gen-only
path: str = "./skip/alabama-gen-only/"
file_list: list = os.listdir(path)

sql_statements_file: TextIO = open("insert-me-alabama.sql", "a")
sql_statements_file_one: TextIO = open("working/insert-me-county-alabama.sql", "a")

# 2020-Primary-Autauga.xls.csv
# Precinct - Columns After ABSENTEE
# County - Autauga County
# State - Alabama
# Jurisdiction - Autauga


def write_candidate(candidate: str):
    insert_statement = f'''
        insert into candidates (name, fec, fec_name) values (\"{candidate.upper()}\", null, null);
    '''

    repo: Dolt = Dolt('working/us-president-precinct-results')

    try:
        # repo.sql(insert_statement)
        logging.info(f"Added Candidate: {candidate.upper()}")
    except DoltException as e:
        None  # Silently Fail As Candidate Already In Database
        # logging.error(f"Insertion Exception: {e}")


def normalize_candidate(candidate: str) -> str:
    new_candidate = candidate.strip()

    if "Donald J. Trump" in new_candidate:
        return "Donald Trump"
    elif "Joseph R. Biden" in new_candidate:
        return "Joseph Biden"
    elif "Bill Weld" in new_candidate:
        return "Bill Weld"
    elif "Michael R. Bloomberg" in new_candidate:
        return "Michael Bloomberg"
    elif "John K. Delaney" in new_candidate:
        return "John Delaney"
    elif "JULIÃN CASTRO" in new_candidate:
        return "JULIAN CASTRO"

    return new_candidate


def write_vote(election_year: int, stage: str, precinct: str, county: str, state: str, jurisdiction: str,
               candidate: str, party: str, writein: int, office: str, vote_mode: str, votes: int):
    logging.debug("-------------------------")
    logging.debug(f"Election Year: {election_year}")
    logging.debug(f"Stage: {stage.lower()}")
    logging.debug(f"Precinct: {precinct.upper()}")
    logging.debug(f"County: {county.upper()}")
    logging.debug(f"State: {state.upper()}")
    logging.debug(f"Jurisdiction: {jurisdiction.upper()}")
    logging.debug(f"Candidate: {candidate.upper()}")
    logging.debug(f"Party: {party.upper()}")
    logging.debug(f"Write-In: {writein}")
    logging.debug(f"Office: {office.upper()}")
    logging.debug(f"Candidate: {candidate.upper()}")
    logging.debug(f"Vote Mode: {vote_mode.upper()}")
    logging.debug(f"Votes: {votes}")
    logging.debug("-------------------------")

    # election_year,stage,precinct,county,state,jurisdiction,candidate,party,writein,office,vote_mode,votes
    insert_statement = f'''
        replace into vote_tallies (election_year,stage,precinct,county,state,jurisdiction,candidate,party,writein,office,vote_mode,votes)
        values ("{election_year}", "{stage.lower()}", "{precinct.upper()}", "{county.upper()}",
        "{state.upper()}", "{jurisdiction.upper()}", "{candidate.upper()}", "{party.upper()}", "{writein}",
        "{office.upper()}", "{vote_mode.upper()}", "{votes}");
    '''

    repo: Dolt = Dolt('working/us-president-precinct-results')

    try:
        # repo.sql(insert_statement)
        sql_statements_file.write(f"{insert_statement}\n")
    except DoltException as e:
        logging.error(f"Insertion Exception: {e}")


def add_county(precinct: str, county: str, state: str, jurisdiction: str):
    logging.debug("-------------------------")
    logging.debug(f"Precinct: {precinct.upper()}")
    logging.debug(f"County: {county.upper()}")
    logging.debug(f"State: {state.upper()}")
    logging.debug(f"Jurisdiction: {jurisdiction.upper()}")
    logging.debug("-------------------------")

    insert_statement = f'''
        replace into precincts (precinct, county, state, jurisdiction) values ("{precinct.upper()}", "{county.upper()}", "{state.upper()}", "{jurisdiction.upper()}");
    '''

    repo: Dolt = Dolt('working/us-president-precinct-results')

    try:
        # var = repo.sql(insert_statement, result_format="json")["rows"]
        sql_statements_file_one.write(f"{insert_statement}\n")
    except DoltException as e:
        None


import_files: list = []
for file in file_list:
    if ".csv" in file:
        import_files.append(file)

for import_file in import_files:
    logging.debug(f"Importing {import_file}")
    file: str = path + import_file
    vote_data: pd.DataFrame = pd.read_csv(file)

    # for precinct in vote_data.keys()[3:]:
    #     try:
    #         county: str = import_file.split("-")[2].split('.')[0]
    #         add_county(precinct=precinct, county=county + " County", state="Alabama", jurisdiction=county)
    #     except:
    #         None

    # break

    # election_year,stage,precinct,county,state,jurisdiction,candidate,party,writein,office,vote_mode,votes
    election_year: int = 2020
    stage: str = "gen".strip()
    county: str = (import_file.split("-")[2].split('.')[0] + " County").strip()
    state: str = "ALABAMA".strip()
    jurisdiction: str = import_file.split("-")[2].split('.')[0].strip()
    write_in: int = 0
    office: str = "US PRESIDENT".strip()
    vote_mode: str = "ELECTION DAY".strip()

    precincts: list[str] = vote_data.keys()[3:]

    for row in vote_data.iterrows():
        contest: str = vote_data.get("Contest Title")[row[0]].strip()

        if "PRESIDENT AND VICE PRESIDENT OF THE UNITED STATES" not in contest:
            continue

        for precinct in precincts:
            # logging.debug(vote_data.get(precinct))
            candidate: str = vote_data.get("Candidate")[row[0]].strip()

            try:
                party: str = vote_data.get("Party")[row[0]].strip()
            except:
                print("Error Party: {eparty}".format(eparty=vote_data.get("Party")[row[0]]))
                continue

            votes: int = vote_data.get(precinct)[row[0]]

            if "Uncommitted" in candidate:
                continue

            if "Over Votes" in candidate:
                continue

            if "Under Votes" in candidate:
                continue

            if int(votes) == 0:
                logging.info(
                    f"Skipping Precinct \"{precinct.upper()}\" For Candidate \"{candidate.upper()}\" because of zero votes!!!")
                continue

            if party == "DEM":
                party = "Democratic".strip()
            elif party == "REP":
                party = "Republican".strip()
            elif party == "IND":
                party = "Independent".strip()
            elif party == "NON":
                party = "Other".strip()
            else:
                logging.debug(f"UNKNOWN PARTY: {party}")
                exit(1)

            candidate = normalize_candidate(candidate=candidate)

            write_candidate(candidate=candidate)

            write_vote(election_year=election_year, stage=stage, county=county, state=state, jurisdiction=jurisdiction,
                       candidate=candidate, party=party, writein=write_in, office=office, vote_mode=vote_mode,
                       precinct=precinct, votes=int(votes))

    # sql_statements_file.close()
