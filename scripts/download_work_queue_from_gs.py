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
    parser.add_argument('--refresh', default = False, action="store_true", help="Instead of overwriting the existing workqueue, add any new items from the worksheet tab.")
    args = parser.parse_args()
    source_spreadsheet, source_worksheet = args.source.split('/')

    # Allow specifying the column to store status in in the worksheet name
    # This only works for columns up to 'Z'
    status_col_name = "B"
    if "@" in source_worksheet:
        source_worksheet, status_col_name = source_worksheet.split('@')
    status_col = ord(status_col_name) - ord('A')

    # This relies on the service json in ~/.config/gspread
    account = gspread.service_account()

    # Get the spreadsheet from Google sheets
    spreadsheet = account.open(source_spreadsheet)

    # Get the worksheet
    worksheet = spreadsheet.worksheet(source_worksheet)

    work_queue_datasets = worksheet.col_values(1)
    work_queue_status = worksheet.col_values(status_col)

    if len(work_queue_datasets) > 1:
        update_values = []
        start_row = 4
        end_row = len(work_queue_datasets)

        open_mode = "a" if args.refresh else "w"
        with open(args.dest, open_mode) as f:
            # Note first row will be the title "dataset"
            for i in range(start_row-1, len(work_queue_datasets)):
                if work_queue_datasets[i] is not None and len(work_queue_datasets[i].strip()) > 0:
                    if args.refresh:
                        # If refreshing, only add datasets with blank statuses
                        if i >= len(work_queue_status) or work_queue_status[i].strip() == '':                     
                            print(f"{work_queue_datasets[i].strip()},IN QUEUE", file=f)
                            update_values.append(["IN QUEUE"])
                        else:
                            # If the status isn't blank, leave it as is
                            update_values.append([work_queue_status[i]])
                    else:
                        # Overwriting and not refreshing, force it to "IN QUEUE"
                        print(f"{work_queue_datasets[i].strip()},IN QUEUE", file=f)
                        update_values.append(["IN QUEUE"])
                else:
                    update_values.append([None])
        
        worksheet.batch_update([{'range': f'{status_col_name}{start_row}:{status_col_name}{end_row}',
                                 'values': update_values}])


if __name__ == '__main__':    
    main()

