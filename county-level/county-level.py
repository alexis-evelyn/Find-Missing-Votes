#!/usr/bin/python
import argparse
import logging
import pandas as pd
import math

from doltpy.core import Dolt, DoltException, system_helpers
from doltpy.core.system_helpers import get_logger

candidates: dict = {
    "Joseph Biden": {"party": "Democratic"},
    "Donald Trump": {"party": "Republican"},
    "Jo Jorgensen": {"party": "Libertarian"},
    "Don Blankenship": {"party": "Constitution Party"},
    "Roque De La Fuente Guerra": {"party": "American Independent"},
    "Howie Hawkins": {"party": "Green"},
    "Gloria La Riva": {"party": "Peace and Freedom"},
    "Brian Carroll": {"party": "American Solidarity Party"},
    "Mark Charles": {"party": "Independent"},
    "Joseph Kishore": {"party": "Socialist Equality Party"},
    "Brock Pierce": {"party": "Life, Liberty, and the pursuit of Happiness"},
    "Jesse Ventura": {"party": "Green"},
    "Other": {"party": "WRITE-IN"},
}

# Repo
repo: Dolt

# Logger Vars
VERBOSE = logging.DEBUG - 1
logging.addLevelName(VERBOSE, "VERBOSE")

INFO_QUIET = logging.INFO + 1
logging.addLevelName(INFO_QUIET, "INFO_QUIET")

# Argument Parser Setup
parser = argparse.ArgumentParser(description='Arguments For Tweet Searcher')
parser.add_argument("-log", "--log", help="Set Log Level (Defaults to INFO_QUIET)",
                    dest='logLevel',
                    default='INFO_QUIET',
                    type=str.upper,
                    choices=['VERBOSE', 'DEBUG', 'INFO', 'INFO_QUIET', 'WARNING', 'ERROR', 'CRITICAL'])

# Logger
logger: logging.Logger = get_logger(__name__)


def main(arguments: argparse.Namespace):
    logger.setLevel(arguments.logLevel)  # This Script's Log Level

    global repo
    repo = create_repo_if_not_exists("./county-level-votes")
    create_tables_if_not_exists()

    parse_and_insert_votes("California", "working/california-18-presidential.csv")
    parse_and_insert_votes("Nevada", "working/nevada.csv")


def create_repo_if_not_exists(path: str) -> Dolt:
    try:
        return Dolt(path)
    except AssertionError:
        return Dolt.init(path)


def create_tables_if_not_exists():
    create_candidate_table_sql: str = '''
        CREATE TABLE if not exists `candidate` (
          `id` bigint NOT NULL AUTO_INCREMENT,
          `name` longtext,
          `fec` longtext,
          PRIMARY KEY (`id`),
          UNIQUE KEY `fec` (`fec`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''

    create_county_table_sql: str = '''
        CREATE TABLE if not exists `county` (
          `id` bigint NOT NULL AUTO_INCREMENT,
          `name` longtext,
          `state` longtext,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''

    create_party_table_sql: str = '''   
        CREATE TABLE if not exists `party` (
          `id` bigint NOT NULL AUTO_INCREMENT,
          `name` longtext,
          PRIMARY KEY (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''

    create_votes_table_sql: str = '''
        CREATE TABLE if not exists `votes` (
          `county` bigint NOT NULL,
          `candidate` bigint NOT NULL,
          `votes` bigint,
          `party` bigint,
          PRIMARY KEY (`county`,`candidate`),
          KEY `candidate` (`candidate`),
          KEY `county` (`county`),
          KEY `party` (`party`),
          CONSTRAINT `FK_Candidate` FOREIGN KEY (`candidate`) REFERENCES `candidate` (`id`),
          CONSTRAINT `FK_County` FOREIGN KEY (`county`) REFERENCES `county` (`id`),
          CONSTRAINT `FK_Parrt` FOREIGN KEY (`party`) REFERENCES `party` (`id`)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    '''

    try:
        repo.sql(create_candidate_table_sql)
    except DoltException:
        logger.warning("Cannot Create Table: Candidate")

    try:
        repo.sql(create_county_table_sql)
    except DoltException:
        logger.warning("Cannot Create Table: County")

    try:
        repo.sql(create_party_table_sql)
    except DoltException:
        logger.warning("Cannot Create Table: Party")

    try:
        repo.sql(create_votes_table_sql)
    except DoltException:
        logger.warning("Cannot Create Table: Votes")


def find_or_add_party(name: str) -> int:
    find_party: str = f'''
        select id from party where name="{name}";
    '''

    rows = repo.sql(find_party, result_format="json")["rows"]

    if len(rows) >= 1:
        return rows[0]['id']

    insert_party: str = f'''
            insert into party (name) values ("{name}");
        '''

    repo.sql(insert_party)

    find_party: str = f'''
        select id from party where name="{name}";
    '''

    rows = repo.sql(find_party, result_format="json")["rows"]

    if len(rows) >= 1:
        return rows[0]['id']

    return -1


def find_or_add_candidate(name: str) -> int:
    find_candidate: str = f'''
        select id from candidate where name="{name}";
    '''

    rows = repo.sql(find_candidate, result_format="json")["rows"]

    if len(rows) >= 1:
        return rows[0]['id']

    insert_candidate: str = f'''
        insert into candidate (name) values ("{name}");
    '''

    repo.sql(insert_candidate)

    find_candidate: str = f'''
        select id from candidate where name="{name}";
    '''

    rows = repo.sql(find_candidate, result_format="json")["rows"]

    if len(rows) >= 1:
        return rows[0]['id']

    return -1


def find_or_add_county(county: str, state: str) -> int:
    find_county: str = f'''
        select id from county where name="{county}" and state="{state}";
    '''

    rows = repo.sql(find_county, result_format="json")["rows"]

    if len(rows) >= 1:
        return rows[0]['id']

    insert_county: str = f'''
        insert into county (name, state) values ("{county}", "{state}");
    '''

    repo.sql(insert_county)

    find_county: str = f'''
        select id from county where name="{county}" and state="{state}";
    '''

    rows = repo.sql(find_county, result_format="json")["rows"]

    if len(rows) >= 1:
        return rows[0]['id']

    return -1


def add_vote(county: int, candidate: int, party: int, votes: int):
    logger.debug(f"County: {county} - Candidate: {candidate} - Party: {party} - Votes: {votes}")

    insert_vote_query: str = '''
        insert into votes (county, candidate, votes, party) values ({county}, {candidate}, {votes}, {party})
    '''.format(county=county, candidate=candidate, votes=votes, party=party)

    try:
        repo.sql(insert_vote_query)
    except DoltException:
        logger.warning(f"Cannot Add Vote!!! County: {county} - Candidate: {candidate} - Party: {party} - Votes: {votes}")


def parse_and_insert_votes(state: str, file: str):
    votes: pd.DataFrame = pd.read_csv(file)

    for index, row in votes.iterrows():
        for candidate in votes.keys()[1:]:
            county: str = row[0]
        
            vote_float: float = float(str(row[candidate]).replace(',', ''))

            if math.isnan(vote_float):
                logger.warning(f"Skipping County: {row[0]} - Candidate: {candidate} Because Count is NaN")
                continue

            count: int = int(math.floor(vote_float))

            if count == 0:
                logger.debug(f"Skipping County: {row[0]} - Candidate: {candidate} Because Count is {count}")
                continue

            county_id: int = find_or_add_county(county=county, state=state)

            if candidate == "Jo Jorgenson":
                candidate = "Jo Jorgensen"

            if "Other" in candidate:
                candidate = "Other"

            candidate_id: int = find_or_add_candidate(name=candidate)

            party: str = ""
            try:
                party: str = candidates[candidate]['party']
            except:
                logger.error(f"Cannot Find Party For Candidate: \"{candidate}\"!!! Exiting!!!")
                exit(1)

            party_id: int = find_or_add_party(name=party)

            logger.log(INFO_QUIET, f"Inserting County ID: {county_id} - Candidate ID: {candidate_id} - Party: {party} - Votes: {count}")
            add_vote(county=county_id, candidate=candidate_id, party=party_id, votes=count)


if __name__ == "__main__":
    # This is to get DoltPy's Logger To Shut Up When Running `this_script.py -h`
    logging.Logger.setLevel(system_helpers.logger, logging.CRITICAL)

    args = parser.parse_args()
    main(args)
