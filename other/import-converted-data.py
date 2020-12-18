import ast
import json
import logging
import os
import csv

from doltpy.core import system_helpers
import pandas as pd



# This is to get DoltPy's Logger To Shut Up When Running `this_script.py -h`
logging.Logger.setLevel(system_helpers.logger, logging.CRITICAL)

path: str = "/Users/alexis/Desktop/bounty/test/output/"
file_list: list = os.listdir(path)

import_files: list = []
for file in file_list:
    if ".csv" in file:
        import_files.append(file)

for import_file in import_files:
    # print(f"Importing {import_file}")
    break

file: str = path + "ak_2016.csv"
# file: str = path + "ar_2016.csv"
file: str = path + "az_2016.csv"

vote_data: pd.DataFrame = pd.read_csv(file, index_col=0)

# print(vote_data.head())

if {'NAME'}.issubset(vote_data):
    print("District Codes!!!")
elif {'COUNTY_NAM'}.issubset(vote_data):
    print("FIPS Codes!!!")
else:
    print(f"Unknown!!! From File: {file}")