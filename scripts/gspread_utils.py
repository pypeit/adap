import gspread
from typing import Optional
import time
import random

def column_name_to_index(column_letter : str) -> int:
    """Returns a column index from a column letter"""
    return  gspread.utils.a1_to_rowcol(column_letter + "1")[1]

def index_to_column_name(index : int) -> str:
    return  gspread.utils.rowcol_to_a1(row=1,col=index)[0]

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



def read_target_coords(gsheet, start_row=4):
    """Read target names and coordinates from a targets tab.

    The tab has the same shape as any other work queue tab -- names in column A from
    start_row down, status and pod in the two columns after the status column -- with the
    coordinates in the two columns after that. With the default status column of B, that
    puts ra in D and dec in E.

    Args:
        gsheet: The spreadsheet, worksheet and optional status column, in the form
                ["key="]spreadsheet["/"worksheet["@"column]].
        start_row: The first row holding a target. Defaults to 4, matching
                   init_work_queue in utils.py.

    Returns:
        A list of dicts with "name", "ra", "dec" and "row" keys, one per non-blank row.
    """
    spreadsheet, worksheet, col_name = open_spreadsheet(gsheet)
    if col_name is None:
        # Default to B if no status column was given
        col_name = "B"

    status_index = column_name_to_index(col_name)

    # The pod name goes immediately right of the status, so the coordinates start after
    # that. Reading whole rows rather than columns keeps this free of the column-letter
    # arithmetic that index_to_column_name gets wrong past column Z.
    ra_index = status_index + 2
    dec_index = status_index + 3

    rows = retry_gspread_call(lambda: worksheet.get_all_values())

    targets = []
    for i in range(start_row - 1, len(rows)):
        row = rows[i]

        name = row[0].strip() if len(row) > 0 else ''
        if name == '':
            continue

        targets.append({'name': name,
                        'ra': row[ra_index - 1].strip() if len(row) >= ra_index else '',
                        'dec': row[dec_index - 1].strip() if len(row) >= dec_index else '',
                        'row': i + 1})

    return targets


def find_target_coords(gsheet, name, start_row=4):
    """Look up one target's row in a targets tab, or None if it is not there."""
    for target in read_target_coords(gsheet, start_row=start_row):
        if target['name'] == name:
            return target

    return None


def append_datasets(gsheet, datasets, start_row=4):
    """Add dataset names to column A of a work queue tab, leaving their status blank.

    A blank status is what makes init_work_queue pick a row up, so appending this way is
    what puts a newly downloaded dataset in line to be reduced. Names already in column A
    are skipped, so re-running a target does not duplicate its rows.

    This is not safe to call concurrently on its own -- read, compute, append is three
    round trips, and two pods interleaving them would overwrite each other. Callers must
    hold the lock belonging to the queue that owns this tab.

    Args:
        gsheet: The spreadsheet and worksheet holding the work queue.
        datasets: Dataset names to add.
        start_row: The first row datasets may occupy. Defaults to 4.

    Returns:
        The names actually added, in the order they were written.
    """
    spreadsheet, worksheet, col_name = open_spreadsheet(gsheet)

    existing = retry_gspread_call(lambda: worksheet.col_values(1))
    have = {value.strip() for value in existing if value.strip() != ''}

    new = []
    for dataset in datasets:
        if dataset not in have:
            new.append(dataset)
            # Guard against the same name arriving twice in one call as well as against
            # one already in the sheet.
            have.add(dataset)

    if len(new) == 0:
        return []

    # col_values stops at the last non-empty cell, so the first free row is one past it --
    # but never above the row the dataset list is defined to start on.
    first_row = max(len(existing) + 1, start_row)
    last_row = first_row + len(new) - 1

    if last_row > worksheet.row_count:
        retry_gspread_call(lambda: worksheet.add_rows(last_row - worksheet.row_count))

    retry_gspread_call(lambda: worksheet.update(range_name=f'A{first_row}:A{last_row}',
                                                values=[[dataset] for dataset in new]))

    return new
