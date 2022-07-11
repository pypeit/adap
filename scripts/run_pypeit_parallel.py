"""
This script runs PypeIt in parallel on multiple pypeit files.

.. include common links, assuming primary doc root is up one directory
.. include:: ../include/links.rst
"""
import subprocess as sp
from time import sleep
from datetime import datetime, timezone
from pathlib import Path
import os

from pypeit.scripts import scriptbase

def log_message(args, msg):
    """Print a message to stdout and to a log file"""
    msg_with_time = datetime.now(timezone.utc).isoformat() + " " + msg
    with open(args.logfile, "a") as f:
        print(msg_with_time, file=f, flush=True)

    print(msg_with_time, flush=True)

def run_pypeit_onfile(file, arguments):
    """
    Run PypeIt on one pypeit file. It makes sure to change the currenct directory to that
    containing the passed in pypeit file.

    Args:
        file (Path):
            Path of the .pypeit file to run PypeIt on
        arguments(list of str):
            Arguments to run_pypeit_parallel from command line.

    Returns:
        Popen : Popen object created when creating the child process to run PypeIt.
    """
       
    pypeit_dir = file.parent
    stdout = pypeit_dir.joinpath("run_pypeit_stdout.txt")
    stdout_file = open(stdout, "w")
    child_env = os.environ.copy()
    # Set the OMP_NUM_THREADS to 1 to prevent numpy multithreading from competing for resources
    # with the multiple processes started by this script
    child_env['OMP_NUM_THREADS'] = '1'

    # Run PypeIt on the pypeit file, using the additional arguments from our command line,
    # with stdout and stderr going to a text file, from the directory of the pypeit file, with
    # the environment set to be single threaded
    p = sp.Popen(["run_pypeit", file.name] + arguments.pypeit_args, stdout=stdout_file, stderr=sp.STDOUT, cwd=pypeit_dir, env=child_env)

    return p



class RunPypeItParallel(scriptbase.ScriptBase):

    @classmethod
    def get_parser(cls, width=None):
        parser = super().get_parser(description='Run PypeIt parallel. This will run PypeIt in one child process per given .pypeit file.',
                                    width=width, formatter=scriptbase.SmartFormatter)

        parser.add_argument('root_dir', type=str,
                            help='Root directory containing pypeit files. This directory is recursively searched and pypeit is run on every .pypeit file found.') 
        parser.add_argument('pypeit_args', type=str, nargs='*', help='Additional arguments and options to pass to PypeIt. See "run_pypeit -h" for more info.')
        parser.add_argument('--num_workers', type=int, help="Number of worker processes to use. If not given, this script will start one process for each pypeit file.")
        parser.add_argument("--logfile", type=str, default="run_pypeit_parallel.log", help= "Log file for woker process status and result information.")
        return parser

    @staticmethod
    def main(args):
        print(f"root_dir: {args.root_dir}")
        print(f"pypeit_args: {args.pypeit_args}")

        # Find the pypeit files to work on
        to_do_files = []
        to_do_files += Path(args.root_dir).rglob("*.pypeit")

        if args.num_workers is None or args.num_workers == 0:
            args.num_workers = len(to_do_files)

        
        
        in_progress_files = []

        while len(to_do_files) > 0 or len(in_progress_files) > 0:
            # Start new workers if needed
            while len(in_progress_files) < args.num_workers and len(to_do_files) > 0:
                next_file = to_do_files.pop()
                try:
                    p = run_pypeit_onfile(next_file, args)
                    in_progress_files.append((next_file, p))
                    msg = f"pid: {p.pid} is running {next_file}"
                    log_message(args,msg)
                except Exception as e:
                    msg = f"Failed to start process for {next_file}: {e}"
                    log_message(args,msg)
                
            # Check workers to see if they've finished
            still_in_progress = []
            while len(in_progress_files) > 0:
                worker = in_progress_files.pop()
                if worker[1].poll() is not None:
                    msg = f"pid: {worker[1].pid} completed {worker[0]}, status: {worker[1].returncode}"
                    log_message(args,msg)
                else:
                    still_in_progress.append(worker)
            in_progress_files = still_in_progress

            sleep(1)

        log_message(args, "Completed all pypeit files.")

if __name__ == '__main__':
    RunPypeItParallel.entry_point()
