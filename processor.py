# module for processing commands in parallel
#
# usage: processor.py [-h] -f FILE
#  required arguments:
#    -f FILE, --file FILE        Input file to load. File should contain a list of commands to execute
#
#  optional arguments:
#  -h, --help                show this help message and exit
#

import logging
import os
import sys
import argparse
import multiprocessing as mp
from time import time

import mytask

# NumSecs to wait for a given process to complete
#    v2: add to config object
TIMEOUT = 35

# Number of jobs to run concurrently
#     v2: make an optional argument. Use cpu_count by default
CONCURRENT_JOBS = 3

# Validate correct path.
#    v2: add to config object
path = ('/usr/sbin', '/bin', '/usr/bin', '/sbin')

# construct a logger for stdout
#    v2: add to config object.
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s|%(levelname)s|%(message)s')


def work(cmd):
    # Inititalizes a task object with a command
    # and executes it using timeout
    task = mytask.Task(cmd)
    task.run(TIMEOUT)

def parse_file(file_):
    # return the trimmed contents of a file
    with open(file_) as f:
        return [line.rstrip() for line in f]

def init():
    '''
    Helper function to validate PATH
    Uses ArgumentParser to initialize with the input file
    Logs relevant bits and returns the file to process
    '''
    validate(path)
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', help="Input file", required=True)
    args = vars(parser.parse_args())
    file_ = args['file']
    logging.info("PLATFORM: %s", sys.platform)
    logging.info("NumCPUs: %s", mp.cpu_count())
    logging.info("InputFile: %s", file_)
    return file_

def validate(vals):
    for s in vals:
        if s not in os.environ['PATH']:
            print("ERROR: {} not in PATH".format(s))
            print("\tRequired values: {}".format(str(vals)))
            sys.exit(1)


if __name__ == '__main__':
    '''
    Initializes execution
    Creates a pool of workers to run tasks in parallel
    Sets a timer for overall execution and logs results
    
    Currently only handles the user hitting Cntrl-C to exit
        v2: Add proper signal handlers for sigterm, etc.
    '''
    srcfile = init()
    pool = mp.Pool(processes=CONCURRENT_JOBS)
    logging.info("Creating process pool with %s workers", CONCURRENT_JOBS)
    mstart = time()
    try:
        # Parse the inputfile and pass the list of commands to the workers
        # for parallel execution
        # Set the timeout value for get() to an arbitrarily high value
        # This is to work around a bug in python v2.7.x where threads don't properly handle KeyboardInterrupt
        runner = pool.map_async(work, parse_file(srcfile))
        ret = (runner.get(1000))
        mruntime = time() - mstart
        logging.info("Execution Stats: NumJobs=%s, Runtime=%s", len(ret), mruntime)
    except KeyboardInterrupt as err:
        # kill remaining jobs and tear down the pool
        pool.terminate()
        logging.info("Process terminated by user")
        sys.exit(0)
