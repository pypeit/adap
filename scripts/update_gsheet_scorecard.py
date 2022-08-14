#!/usr/bin/env python3


import time
import random
import argparse
import csv
from datetime import date

import numpy as np
import gspread

MAX_SCORECARD_COL = 'W'
def split_to_csv_tabs(t, outpath):
    """Split the score card table into smaller CSVs that will go into tabs in the Google sheet.
       The datasets are split based on the first letter of the mask name, according to ranges 
       that were determined by looking at the number of raw images per datset.
       
       Args:
       t (astropy.table.Table): The complete scorecard table.

       outpath (pathlib.Path): The path where the csv files should be go.

       Returns None. (But the csv files are written).
       """
    alphabet_ranges = [('0', '9'),
                       ('A', 'B'),
                       ('C', 'C'),
                       ('D', 'F'),
                       ('G', 'H'),
                       ('I', 'L'),
                       ('M', 'M'),
                       ('N', 'R'),
                       ('S', 'S'),
                       ('T', 'V'),
                       ('W', 'Z')]

    for range in alphabet_ranges:
        idx =[x[0].upper() >= range[0] and x[0].upper() <= range[1] for x in t['dataset']]
        if np.sum(idx) > 0:
            # The file will be something like 'scorecard_A-B.csv' for multi letter ranges, or
            # 'scorecard_C.csv' for single letter ranges
            t[idx].write(outpath / f"scorecard_{f'{range[0]}-{range[1]}' if range[0] != range[1] else range[0]}.csv", overwrite=True)

def retry_gspread_call(func, retry_delays = [30, 60, 60, 90], retry_jitter=5):

    for i in range(len(retry_delays)+1):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if i == len(retry_delays):
                # We've passed the max # of retries, re-reaise the exception
                raise
            time.sleep(retry_delays[i] + random.randrange(1, retry_jitter+1))

def build_array_from_rows(data_rows):
    scorecard_dtypes = ['U64', 'U22', 'U40', 'U8'] + [int for x in data_rows[0][4:-2]] + ['datetime64[D]', 'U20']
    scorecard_names = data_rows[0]
    # numpy requires tuples for structured arrays
    scorecard_values = [tuple(x) for x in data_rows[1:]]
    data_array = np.array(scorecard_values, dtype=list(zip(scorecard_names, scorecard_dtypes)))
    return data_array

def convert_array_to_gspread(data):

    # Convert from ndarray to list
    list_data = data.tolist()

    # Convert any dates to strings
    date_index = data.dtype.names.index('date')
    # We need to use lists of lists instead of lists of tuples in order to modify the data
    list_of_list = []
    for row in list_data:
        list_of_list.append(list(row))
        if isinstance(row[date_index], date):
            list_of_list[-1][date_index] = row[date_index].isoformat()
    return list_of_list

def update_worksheet_with_dataset(args, worksheet, worksheet_array, dataset_csv_array):

    if len(dataset_csv_array) == 0:
        # Nothing to do
        return

    # Note don't need to refresh the worksheet values after inserting/deleting new items
    # because this function is called on datasets in reverse order. This means the
    # indices changed are always after where the next dataset goes
    dataset = dataset_csv_array[0]['dataset']
    worksheet_indices = np.nonzero(worksheet_array['dataset'] == dataset)[0]

    if len(worksheet_indices) == 0:
        # The dataset is not in the worksheet

        if len(worksheet_array) > 0:
            # Look for where the dataset should be inserted
            worksheet_upper_datasets = np.copy(worksheet_array['dataset'])
            worksheet_upper_datasets = [d.upper() for d in worksheet_upper_datasets]
            # We add two because of the missing header line from the array and because
            # The spreadsheets indices are 1 based
            start = np.searchsorted(worksheet_upper_datasets, dataset.upper()) + 2
        else:
            # Put at top 
            start = 2

        insert_rows = len(dataset_csv_array)        
        update_rows = 0
        delete_rows = 0

    elif len(worksheet_indices) < len(dataset_csv_array):
        # Insert rows to make the new dataset fit
        start=worksheet_indices[0]+2
        insert_rows = len(dataset_csv_array) - len(worksheet_indices)
        update_rows = len(dataset_csv_array) - insert_rows
        delete_rows = 0
    elif len(worksheet_indices) > len(dataset_csv_array):
        # Delete rows to make the new dataset fit
        start=worksheet_indices[0]+2
        insert_rows=0
        update_rows = len(dataset_csv_array)
        delete_rows = len(worksheet_indices) - len(dataset_csv_array)
    else:
        # They are equal
        start=worksheet_indices[0]+2
        insert_rows = 0
        update_rows = len(dataset_csv_array)
        delete_rows = 0

    # We get np.int64 from numpy, but google hates that (won't convert it to JSON for its api)
    start =int(start)
    insert_rows = int(insert_rows)
    update_rows = int(update_rows)
    delete_rows = int(delete_rows)

    next_csv_row = 0
    if insert_rows != 0:
        retry_gspread_call(lambda: worksheet.insert_rows(convert_array_to_gspread(dataset_csv_array[next_csv_row:next_csv_row+insert_rows]), row=start, value_input_option='USER_ENTERED'))
        next_csv_row += insert_rows
        start += insert_rows
    if update_rows != 0:
        range = f'A{start}:{MAX_SCORECARD_COL}{start+update_rows-1}'
        retry_gspread_call(lambda: worksheet.update(range, convert_array_to_gspread(dataset_csv_array[next_csv_row:next_csv_row+update_rows]), value_input_option='USER_ENTERED'))
        next_csv_row += update_rows
        start += update_rows
    if delete_rows != 0:
        retry_gspread_call(lambda: worksheet.delete_rows(start, start+delete_rows-1))


def update_gsheet_worksheet(args, spreadsheet, worksheet_name, csv_array):

    # Get the worksheet
    worksheet = retry_gspread_call(lambda: spreadsheet.worksheet(worksheet_name))
    worksheet_rows = retry_gspread_call(lambda: worksheet.get_values())

    if len(worksheet_rows[0]) != len(csv_array.dtype.names):
        raise ValueError(f"CSV file does not match the columns in {args.spreadsheet}/{worksheet_name}")

    unfiltered_worksheet_array = build_array_from_rows(worksheet_rows)

    if worksheet_name == "latest":
        # Filter out older values
        # "older" is more than args.latest_days before the oldest item in the new data.
        # This prevents the sheet from being empty when there's a puase in reductions
        oldest_csv_date = np.min(csv_array['date'])
        old_idx = unfiltered_worksheet_array['date'] < oldest_csv_date - np.timedelta64(args.latest_days, 'D')
        
        if np.any(old_idx):
            # Filter out the old ones without refetching the array after we delete them
            worksheet_array = unfiltered_worksheet_array[np.logical_not(old_idx)]

            # Use np.unique to group the old entries, to try to minimize the # of delete operations we
            # do.
            x, old_indices, old_counts = np.unique(old_idx, return_index=True, return_counts=True)
            for i in np.nonzero(x == True)[0]:
                start = old_indices[i] + 2
                end = start + old_counts[i] - 1
                retry_gspread_call(lambda: worksheet.delete_rows(int(start), int(end)))

        else:
            # No need to filter out old entries
            worksheet_array = unfiltered_worksheet_array    
    else:
        worksheet_array = unfiltered_worksheet_array    

    # Group the rows by dataset using unique
    unique_datasets, dataset_start_indices, dataset_counts = np.unique(csv_array['dataset'], return_index=True, return_counts=True)

    sort_idx = dataset_start_indices.argsort()[::-1]

    for idx in sort_idx:
        start = dataset_start_indices[idx]
        count = dataset_counts[idx]
        update_worksheet_with_dataset(args, worksheet, worksheet_array, csv_array[start:start+count])





def dataset_sheet_filter(sheet, data):
    if sheet == 'Failed':
        return data[data['status'] == 'FAILED']
    elif sheet == 'latest':
        return data
    else:
        letters = sheet.split('-')
        start = letters[0]
        end = letters[-1]
        if start == end:
            return data[[dataset[0].upper() == start for dataset in data['dataset']]]
        else:
            return data[[dataset[0].upper() >= start and dataset[0].upper() <= end for dataset in data['dataset']]]


def main():
    parser = argparse.ArgumentParser(description='Update the ADAPs Google scorecard spreadsheet for completed pypeit reductions.')
    parser.add_argument("spreadsheet", type=str, help = "Name of the spreadsheet in Google Drive.")
    parser.add_argument("scorecard", type=str, help='Output csv file.')
    parser.add_argument("latest_days", type=int, default = 5, help='The number of days to keep in the "latest" tab of the scorecard., defaults to 5')
    args = parser.parse_args()


    sheets = ['latest', 'Failed', '0-9', 'A-B', 'C', 'D-F', 'G-H', 'I-L', 'M','N-R', 'S', 'T-V', 'W-Z']
    

    print (f"Reading {args.scorecard}")
    csv_rows = []
    with open(args.scorecard, "r", newline='\n') as f:
        reader = csv.reader(f)
        csv_rows = list(reader)

    csv_array=build_array_from_rows(csv_rows)

    if len(csv_array) == 0:
        # Nothing to do
        print(f"CSV is empty, nothing to do.")
        return 0

    # Do a case insensitive sort by using argsort on a new array of uppercase values
    csv_keys = np.copy(csv_array[['dataset', 'science_file', 'reduce_dir']])
    csv_keys['dataset'] = [d.upper() for d in csv_keys['dataset']]
    sort_idx = csv_keys.argsort(order=['dataset', 'science_file', 'reduce_dir'])


    print (f"Accessing {args.spreadsheet} in G-Drive")
    # This relies on the service json in ~/.config/gspread
    account = gspread.service_account()

    # Get the spreadsheet from Google sheets
    spreadsheet = account.open(args.spreadsheet)


    for sheet in sheets:
        print(f"Updating {sheet}")
        update_gsheet_worksheet(args, spreadsheet, sheet, dataset_sheet_filter(sheet, csv_array[sort_idx]))


if __name__ == '__main__':    
    main()

