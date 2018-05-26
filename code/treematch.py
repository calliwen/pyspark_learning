# Copyright 2018 The LogPAI Team (https://github.com/logpai).
#
# Licensed under the MIT License:
# Permission is hereby granted, free of charge, to any person obtaining a copy 
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights 
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell 
# copies of the Software, and to permit persons to whom the Software is 
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in 
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
# SOFTWARE.
# =============================================================================
""" 
This file implements the regular expression based algorithm for log 
template matching. The algorithm is described in the following paper:
[1] Jieming Zhu, Jingyang Liu, Pinjia He, Zibin Zheng, Michael R. Lyu. 
    "Real-Time Log Event Matching at Scale", XXX, 2018.
"""

import logloader
from collections import defaultdict, Counter, OrderedDict
import re
import pandas as pd
import os
import sys
from datetime import datetime
import multiprocessing as mp
import itertools
import hashlib
import string
import numpy as np



def message_split(message):
    message = re.sub(r"<+", " <", message)
    message = re.sub(r">+", "> ", message)
    punc = re.sub('[*<>]', "", string.punctuation)
    splitters = '\s\\' + '\\'.join(punc)
    splitter_regex = re.compile('[{}]+'.format(splitters))
    splitter_regex = re.compile('[^*<>A-Za-z]+')
    tokens = re.split(splitter_regex, message)
    tokens = [token for idx, token in enumerate(tokens) if token!='' and not (token == "<*>" and idx > 0 and tokens[idx-1]=="<*>")]
    return tokens

def tree_match(match_tree, log_list):
    print("Worker {} start matching {} lines.".format(os.getpid(), len(log_list)))
    log_template_dict = {}
    for log_content in log_list:
        # if "Jetty" not in log_content: continue
        # Full match
        if log_content in match_tree["$NO_STAR$"]:
            log_template_dict[log_content] = log_content
            continue
        log_tokens = message_split(log_content)
        template = match_template(match_tree, log_tokens)
        log_template_dict[log_content] = template if template else "NoMatch"
    return log_template_dict

def match_template(match_tree, log_tokens):
    result = []
    find_template(match_tree, log_tokens, result)
    if result:
        result.sort(key=lambda x: (-x[1][0], x[1][1]))
        return result[0][0]
    return None

def find_template(move_tree, log_tokens, result):
    if len(log_tokens) == 0:
        for key, value in move_tree.items():
            if isinstance(value, tuple):
                result.append((key, value))
        if "<*>" in move_tree:
            move_tree = move_tree["<*>"]
            for key, value in move_tree.items():
                if isinstance(value, tuple):
                    result.append((key, value))
        return
    token = log_tokens[0]
    # print("---")
    # print(log_tokens)
    # print(token)
    # print(move_tree)
    # print("---")
    if token in move_tree:
        find_template(move_tree[token], log_tokens[1:], result)
    if "<*>" in move_tree:
        if isinstance(move_tree["<*>"], dict):
            next_keys = move_tree["<*>"].keys()
            next_continue_keys = []
            for nk in next_keys:
                nv = move_tree["<*>"][nk]
                if isinstance(nv, tuple):
                    result.append((nk, nv))
                else:
                    next_continue_keys.append(nk)
            idx = 0
            while idx < len(log_tokens):
                token = log_tokens[idx]
                if token in next_continue_keys:
                    find_template(move_tree["<*>"], log_tokens[idx:], result)
                idx += 1
            if idx == len(log_tokens):
                find_template(move_tree, log_tokens[idx+1:], result)
        else:
            result.append(("<*>", move_tree["<*>"]))



class PatternMatch(object):
    def __init__(self, outdir='./result/', n_workers=1, optimized=False, logformat=None):
        self.outdir = outdir
        if not os.path.exists(outdir):
            os.makedirs(outdir) # Make the result directory
        self.logformat = logformat
        self.n_workers = n_workers

    def match(self, log_filepath, template_filepath):
        print('Processing log file: {}...'.format(log_filepath))
        start_time = datetime.now()
        loader = logloader.LogLoader(self.logformat, self.n_workers)
        log_dataframe = loader.load_to_dataframe(log_filepath)

        print('Reading templates from {}...'.format(template_filepath))
        templates = self._read_template_from_csv(template_filepath)
        print('Building match tree...')
        match_tree = self._build_match_tree(templates)

        print('Matching event templates...')
        match_dict = self.match_event(match_tree, log_dataframe['Content'].tolist())
        
        log_dataframe['EventTemplate'] = log_dataframe['Content'].map(match_dict)
        log_dataframe['EventId'] = log_dataframe['Content'].map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()[0:8])
        
        # wrtie csv to path ~ 
        self._dump_match_result(os.path.basename(log_filepath), log_dataframe)
        match_rate = sum(log_dataframe['EventTemplate'] != 'NoMatch') / float(len(log_dataframe))
        
        print('Matching done, matching rate: {:.1%} [Time taken: {!s}]'.format(match_rate, datetime.now() - start_time))
        return log_dataframe


    def match_event(self, match_tree, log_list):
        log_template_dict = tree_match(match_tree, log_list)
        return log_template_dict


    def _build_match_tree(self, templates):
        match_tree = {}
        match_tree["$NO_STAR$"] = {}
        for event_id, event_template in templates:
            # Full match
            if "<*>" not in event_template:
                match_tree["$NO_STAR$"][event_template] = event_template
                continue
            event_template = re.sub("<NUM>", "<*>", event_template)
            template_tokens = message_split(event_template)
            # print("Templates", template_tokens)
            if not template_tokens: continue

            start_token = template_tokens[0]
            if start_token not in match_tree:
                match_tree[start_token] = {}
            move_tree = match_tree[start_token]

            tidx = 1
            while tidx < len(template_tokens):
                token = template_tokens[tidx]
                if token not in move_tree:
                    move_tree[token] = {}
                move_tree = move_tree[token]
                tidx += 1
            move_tree[event_template] = (len(template_tokens), template_tokens.count("<*>")) # length, count of <*>
        # print(match_tree)
        # sys.exit()
        return match_tree

    def _read_template_from_csv(self, template_filepath):
        template_dataframe = pd.read_csv(template_filepath)
        templates = []
        for idx, row in template_dataframe.iterrows():
            event_id = row['EventId']
            event_template = row['EventTemplate']
            templates.append((event_id, event_template))
        return templates

    def _dump_match_result(self, log_filename, log_dataframe):
        log_dataframe.to_csv(os.path.join(self.outdir, log_filename + '_structured.csv'), index=False)
        occ_dict = dict(log_dataframe['EventTemplate'].value_counts())
        template_df = pd.DataFrame()
        template_df['EventTemplate'] = log_dataframe['EventTemplate'].unique()
        template_df['EventId'] = template_df['EventTemplate'].map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest()[0:8])
        template_df['Occurrences'] = template_df['EventTemplate'].map(occ_dict)
        template_df.to_csv(os.path.join(self.outdir, log_filename + '_templates.csv'), index=False, columns=['EventId', 'EventTemplate', 'Occurrences'])

