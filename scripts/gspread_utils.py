import gspread
from typing import Optional
import time
import random

def column_name_to_index(column_letter : str) -> int:
    """Returns a column index from a column letter"""
    return  (ord(column_letter) - ord('A'))+1

def signal_proof_sleep(seconds):
    # I've noticed the time.sleep() function doesn't alway sleep as long as I want. My theory,
    # based on the docs, is that some network errors contacting S3/Google Drive cause a signal
    # which raises an exception. In any event this code make sure that the retries sleep for
    # the desired # of seconds.
    start_time = time.time()
    current_time = start_time
    while current_time < start_time + seconds:
        time.sleep(1)
        current_time = time.time()


def retry_gspread_call(func, retry_delays = [30, 60, 60, 90], retry_jitter=5):

    for i in range(len(retry_delays)+1):
        try:
            return func()
        except gspread.exceptions.APIError as e:
            if i == len(retry_delays):
                # We've passed the max # of retries, re-reaise the exception
                raise
        except:
            # an exception type we don't want to retry
            raise
        
        # A failure happened, sleep before retrying
        signal_proof_sleep(retry_delays[i] + random.randrange(1, retry_jitter+1))



def open_spreadsheet(name : str)->tuple[gspread.spreadsheet.Spreadsheet,
                                   Optional[gspread.worksheet.Worksheet], 
                                   Optional[str]]:
    """Open a google sheets spreadsheet or worksheet.
    
    Args:
        name: The name of the spreadsheet, with optional worksheet and column
              attached, in the format::
              
              ["key="]spreadsheet_name ["/" worksheet_name ["@" column_letter]]

              The spreadsheet_name can also be specified as a key by prefixing
              with "key=". This key can be found in the spreadsheet's URL and can be
              useful for spreadsheets in shared drives.

    Returns:
        spreadsheet:  The gspread spreadsheet object.
        worksheet:    The gspread worksheet object, or None if none was specified.
        column_name:  The name of the column specified, or None if no column was specified.
    """
    # Allow specifying the column to store status in in the worksheet name
    # This only works for columns up to 'Z'
    column_name = None
    worksheet = None

    name_parts = name.split('/')

    spreadsheet_name = name_parts[0]
    if spreadsheet_name.startswith("key="):
        spreadsheet_key = spreadsheet_name[4:]
    else:
        spreadsheet_key = None

    if len(name_parts) >= 2:
        worksheet_name = name_parts[1]
        if "@" in worksheet_name:
            worksheet_name, column_name = worksheet_name.split('@')
    else:
        worksheet_name = None

    # This relies on the service json in ~/.config/gspread
    account = gspread.service_account()

    # Get the spreadsheet from Google sheets
    if spreadsheet_key is not None:
        spreadsheet = retry_gspread_call(lambda: account.open_by_key(spreadsheet_key))
    else:
        spreadsheet = retry_gspread_call(lambda: account.open(spreadsheet_name))

    if worksheet_name is not None:
        worksheet = retry_gspread_call(lambda: spreadsheet.worksheet(worksheet_name))

    return spreadsheet, worksheet, column_name

