#!/usr/bin/env python

import sys
import pandas as pd
import re
import multiprocessing as mp
from itertools import groupby, count, chain
import numpy as np

import hashlib

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType, ArrayType
from pyspark.sql.functions import *

from logloader import formalize_line, LogLoader
from treematch import PatternMatch, message_split, match_template


if __name__ == "__main__":   
    
    logfile_dir = '../logs/' # The input directory
    template_dir = "../template/"
    output_dir   = '../logmatch_result/' # The result directory
    
    log_name = 'HDFS_2k.log' # The input log file path
    template_name =  log_name + '_templates.csv' # The event template file path

    log_format   = '<Date> <Time> <Pid> <Level> <Component>: <Content>' # HDFS log format
    n_workers    = 1 # The number of workers in parallel
    
    print( "start set sparkSession..." )
    spark = SparkSession.builder.appName( "sparkStream" ).getOrCreate()
    lines = spark.readStream.text( logfile_dir )

    print( "start to load log_format..." )
    # read log and analyse template
    log_load = LogLoader(log_format, n_workers = n_workers)
    headers, regex = log_load.headers, log_load.regex
    print( "regex type: ", type(regex) )
    print( "headers type: ", type(headers) )
    print( "headers: ", headers)  #  ['Date', 'Time', 'Pid', 'Level', 'Component', 'Content']
    split_line = udf( lambda line: formalize_line(line, regex, headers), StringType() )


    def split_msg(line):
        line_list = line.split("--#--")
        # print( "line len: ", len(line_list) )
        res = ''
        if len(line_list) < 6:
            pass
        else:
            res = line_list[5]  
        return res
    split_message = udf( lambda line: split_msg(line)  )
    
    
    # treematch PatternMatch
    patn_match = PatternMatch( outdir=output_dir, n_workers=1, optimized=True,)
    templates = patn_match._read_template_from_csv( template_dir + template_name )
    match_tree = patn_match._build_match_tree(templates)

    # match_dict = patn_match.match_event(match_tree, log_dataframe['Content'].tolist())
    def match_content(log_content, match_tree):
        if log_content in match_tree["$NO_STAR$"]:
            # log_template_dict[log_content] = log_content
            return log_content
        else:
            log_tokens = message_split(log_content)
            template = match_template(match_tree, log_tokens)
            # log_template_dict[log_content] = template if template else "NoMatch"
            res = template if template else "NoMatch"
            return res 
    set_EventTemplate = udf( lambda content: match_content( content, match_tree ) )
    set_EventId = udf( lambda EventTemplate: hashlib.md5(EventTemplate.encode('utf-8')).hexdigest()[0:8] )

    print( "start to line.select..." )
    res_data = lines.select("value")\
                      .withColumn( "value" , split_line("value") )\
                      .withColumn( "content", split_message('value'))\
                      .withColumn( "EventTemplate", set_EventTemplate("content") )\
                      .withColumn( "EventId", set_EventId("EventTemplate") )
    
    def set_col(line, col_id, sep="--#--"):
        return line.split(sep)[col_id]

    # set headers 
    set_Date =      udf( lambda val_line: set_col( val_line, 0 ), StringType() )
    set_Time =      udf( lambda val_line: set_col( val_line, 1 ), StringType() )
    set_Pid =       udf( lambda val_line: set_col( val_line, 2 ), StringType() )
    set_Level =     udf( lambda val_line: set_col( val_line, 3 ), StringType() )
    set_Component = udf( lambda val_line: set_col( val_line, 4 ), StringType() )
    set_Content =   udf( lambda val_line: set_col( val_line, 5 ), StringType() )

    # set match_list
    set_EventId =       udf( lambda match_line: set_col( match_line, 0 ), StringType() )
    set_EventTemplate = udf( lambda match_line: set_col( match_line, 1 ), StringType() )


    res_data = res_data.select( ['value', "EventId", "EventTemplate" ] )\
                        .withColumn( 'Date', set_Date('value') )\
                        .withColumn( 'Time', set_Time('value') )\
                        .withColumn( 'Pid', set_Pid('value') )\
                        .withColumn( 'Level', set_Level('value') )\
                        .withColumn( 'Component', set_Component('value') )\
                        .withColumn( 'Content', set_Content('value') ) # \
    
    res_data = res_data.drop( 'value' ) # .drop('match_list')


    query = res_data.writeStream\
                .format('console')\
                .outputMode('append')\
                .start()
    print( "query to console!" )
    query.awaitTermination()
    print( "done!" )

    
    # query = res_data.writeStream\
    #                 .format('csv')\
    #                 .option('path', './message_to_df/')\
    #                 .option('checkpointLocation', "./checkpoint/")\
    #                 .start()
    # query.awaitTermination()
    # print( "done!" )

    