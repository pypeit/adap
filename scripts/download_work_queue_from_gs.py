"""
Down load a work queue from a google sheet.

This is part of a work flow for running PypeIt in parallel in kubernetes.
The file created by this script is used by each pod to determine the next dataset to reduce.
Using a google sheet is easy for users, but the Google API doesn't really the locking needed to prevent
race conditions. So the work queue is downloaded into a file in  a persistent volume that can be locked.
"""
import argparse
import gspread

def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.\n Authentication requres a "service_account.json" file in "~/.config/gspread/".')
    parser.add_argument('source', type=str, help="Source Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('dest', type=str, help="File on local file system to write the work queue to.")

    args = parser.parse_args()
    source_spreadsheet, source_worksheet = args.source.split('/')
    # This relies on the service json in ~/.config/gspread
    account = gspread.service_account()

    # Get the spreadsheet from Google sheets
    spreadsheet = account.open(source_spreadsheet)

    # Get the worksheet
    worksheet = spreadsheet.worksheet(source_worksheet)

    work_queue = worksheet.col_values(1)

    if len(work_queue) > 1:
        update_values = []
        start_row = 4
        end_row = len(work_queue)

        with open(args.dest, "w") as f:
            # Note first row will be the title "dataset"
            for i in range(start_row-1, len(work_queue)):
                if work_queue[i] is not None and len(work_queue[i].strip()) > 0:
                    print(f"{work_queue[i].strip()},IN QUEUE", file=f)
                    update_values.append(["IN QUEUE"])
                else:
                    update_values.append([None])
        
        worksheet.batch_update([{'range': f'B{start_row}:B{end_row}',
                                 'values': update_values}])


if __name__ == '__main__':    
    main()

