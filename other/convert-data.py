import ast
import json
import logging
import os

from doltpy.core import system_helpers
import pandas as pd

# This is to get DoltPy's Logger To Shut Up When Running `this_script.py -h`
logging.Logger.setLevel(system_helpers.logger, logging.CRITICAL)

path: str = "/Users/alexis/Desktop/bounty/test/"
file_list: list = os.listdir(path)
# "ak_2016.csv"

import_files: list = []
for file in file_list:
    if ".csv" in file:
        import_files.append(file)

# print(import_files)
for import_file in import_files:
    print(f"Importing {import_file}")
    vote_dict: list = []
    with open(path + import_file, 'r') as file:
        lines: list[str] = file.readlines()

        count: int = 0
        for line in lines:
            try:
                row: dict = ast.literal_eval(line)
                vote_dict.append(row)
            except ValueError as e:
                print(f"Could Not Parse Line '{line}' From File '{import_file}' Due To ValueError '{e}'")

    # Close File
    file.close()

    # DataFrame
    vote_data: pd.DataFrame = pd.DataFrame(vote_dict)

    # print(vote_data)
    print(f"Exporting File: output/{import_file}")
    vote_data.to_csv(path + "output/" + import_file)
