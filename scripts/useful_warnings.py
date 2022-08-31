import os
import argparse
import numpy as np

from IPython import embed


def read_lines(file):
    """Short helper method to read lines from a text file into a list, removing newlines."""
    with open(file, "r") as f:
        lines = [line.rstrip() for line in f]
    return lines


def required_warns(file):
    """ Read and return the most useful warnings from a text file.
    The location of the file is "adap/config/required_warnings.txt".

    """
    lines = np.array(read_lines(file))
    # .py file where the msg if generated
    code_file = np.array([ll.split(' ')[0] for ll in lines])
    # method where the msg is generated
    method = np.array([ll.split(' ')[1] for ll in lines])
    # warning msg
    msg = [np.array(ll.split(' ')[3:]) for ll in lines]
    # remove white spaces that are counted as elements of msg
    msg = np.array([m[m != ''] for m in msg], dtype=object)

    return code_file, method, msg


def warns_fromlogfile(logfile):
    """ Read and return the warnings from the .log file.

    """
    lines = np.array(read_lines(logfile))
    # index lines with warning
    widx = np.where(np.array([ll.find('[WARNING]') for ll in lines]) > -1)[0]
    # only lines with warnings
    all_warn_lines = lines[widx]
    # .py file where the msg if generated
    code_file = np.array([ll.split(' ')[2] for ll in all_warn_lines])
    # method where the msg is generated
    method = np.array([ll.split(' ')[4] for ll in all_warn_lines])
    # warning msg
    msg = [np.array(ll.split(' ')[6:]) for ll in all_warn_lines]
    # remove white spaces that are counted as elements of msg
    msg = np.array([m[m != ''] for m in msg], dtype=object)

    return widx, all_warn_lines, code_file, method, msg


def main():
    parser = argparse.ArgumentParser(description='Parse and save in a new txt file the most useful '
                                                 'warnings from the .log file')
    parser.add_argument("logfile", type=str, default="keck_deimos_A.log",
                        help="Location and name of the log file")
    parser.add_argument("--req_warn_file", type=str, default="adap/config/required_warnings.txt",
                        help="Location and name of the file containing the required warnings")

    args = parser.parse_args()

    # read in the required warning that we want to find in the log file
    req_code_file, req_method, req_msg = required_warns(args.req_warn_file)

    # read in the warning from the log file
    line_number, log_warn_lines, log_code_file, log_method, log_msg = warns_fromlogfile(args.logfile)

    # most important warnings log file
    useful_warns = args.logfile.split('.log')[0] + '_useful_warns.log'
    with open(useful_warns, 'w') as f:
        for i in range(log_code_file.size):
            for j in range(req_code_file.size):
                # if log_code_file does not include .py means that this line is probably a
                # continuation of a previous line, so we skip it
                if '.py' not in log_code_file[i]:
                    continue
                if (req_code_file[j] == log_code_file[i]) & (req_method[j] == log_method[i]) & \
                        np.all([ll in log_msg[i] for ll in req_msg[j]]):
                    f.write(f'{line_number[i]+1} -- {log_warn_lines[i]}\n')


if __name__ == '__main__':
    main()



