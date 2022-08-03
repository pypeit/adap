"""
"""
import argparse
import os
import csv
import io
import gspread


def update_gsheet(spreadsheet, dataset, pod_name):


    print(f"{pod_name} updating scorecard")
    source_spreadsheet, source_worksheet = spreadsheet.split('/')

    # This relies on the service json in ~/.config/gspread
    account = gspread.service_account()

    # Get the spreadsheet from Google sheets
    spreadsheet = account.open(source_spreadsheet)

    # Get the worksheet
    worksheet = spreadsheet.worksheet(source_worksheet)

    work_queue = worksheet.col_values(1)

    if len(work_queue) > 1:

        # Note first row will be the title "dataset"
        for i in range(1, len(work_queue)):
            if work_queue[i].strip() == dataset:
                print(f"{pod_name} Found {dataset} in scorecard.")
                worksheet.update(f"B{i+1}", pod_name)
                break
        




def main():
    parser = argparse.ArgumentParser(description='Download the ADAP work queue from Google Sheets.\n Authentication requres a "service_account.json" file in "~/.config/gspread/".')
    parser.add_argument('gsheet', type=str, help="Scorecard Google Spreadsheet and Worksheet. For example: spreadsheet/worksheet")
    parser.add_argument('work_queue', type=str, help="CSV file containing the work queue.")

    args = parser.parse_args()

    my_pod = os.environ["POD_NAME"]

    try:
        fd = os.open(args.work_queue, os.O_RDWR)
        os.lockf(fd, os.F_LOCK, 0)
        print(f"Pod {my_pod} Reading work queue")
        file = open(fd, "r", closefd=False)
        csv_reader = csv.reader(file)
        rows = []
        found=False        
        for row in csv_reader:
            rows.append(row)
            if not found and row[1] == 'IN QUEUE':
                row[1] = my_pod
                dataset = row[0]
                print(f"Pod ${my_pod} claiming dataset {dataset}")
                found = True

        file.close()

        file=open(fd, "w", closefd= False)
        file.truncate(0)
        file.seek(0,io.SEEK_SET)
        csv_writer = csv.writer(file)
        csv_writer.writerows(rows)
        file.close()

        if found:
            update_gsheet(args.gsheet, dataset, my_pod)


    finally:
        os.close(fd)

if __name__ == '__main__':    
    main()

