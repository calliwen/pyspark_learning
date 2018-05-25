#!/usr/bin/env python
import sys
sys.path.append('../')
from fastmatch import treematch

input_dir    = '../logs/' # The input directory
output_dir   = 'result/' # The result directory
log_filepath = input_dir + 'HDFS_2k.log' # The input log file path
log_format   = '<Date> <Time> <Pid> <Level> <Component>: <Content>' # HDFS log format
n_workers    = 1 # The number of workers in parallel
template_filepath = log_filepath + '_templates.csv' # The event template file path


if __name__ == "__main__":
    matcher = treematch.PatternMatch(outdir=output_dir, n_workers=n_workers, logformat=log_format)
    matcher.match(log_filepath, template_filepath)