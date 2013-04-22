#!/usr/bin/env python
# accumulate_fsu_stats.py
#*****************************************************************************\
#*  $Id: Makefile,v 1.9 2013/04/22 auselton Exp $
#*****************************************************************************
#*  Copyright (C) 2013 The Regents of the University of California.
#*  Produced at Lawrence Berkeley National Laboratory (cf, DISCLAIMER).
#*  Written by Andrew Uselton <acuselton@lbl.gov> as part of LMT:
#*  Copyright (C) 2007-2010 Lawrence Livermore National Security, LLC.
#*  This module (re)written by Jim Garlick <garlick@llnl.gov>.
#*  UCRL-CODE-232438
#*
#*  This file is part of Lustre Monitoring Tool, version 2.
#*  Authors: H. Wartens, P. Spencer, N. O'Neill, J. Long, J. Garlick
#*  For details, see http://github.com/chaos/lmt.
#*
#*  LMT is free software; you can redistribute it and/or modify it under
#*  the terms of the GNU General Public License (as published by the Free
#*  Software Foundation) version 2, dated June 1991.
#*
#*  LMT is distributed in the hope that it will be useful, but WITHOUT
#*  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
#*  FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
#*  for more details.
#*
#*  You should have received a copy of the GNU General Public License along
#*  with LMT; if not, write to the Free Software Foundation, Inc.,
#*  59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.
#*****************************************************************************/
# In this case there is only one line of summary statistics in stead of
# two.
#   parse one *_fsu_stats.report file and add it to another. Create the
# target if it does not already exist.

import os
import sys
import argparse
import numpy as np
import datetime
import time
import readline

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    The command line arguments needed for operating the OST class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-a', '--add', default=None, type=str, help='The target file to use')
    parser.add_argument('-f', '--file', default=None, type=str, help='The source file to use')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        print "Please provide a source file (-f)"
    return(args)

#*******************************************************************************

def parse_stats(stats_line):
    stats = {}
    stats_list = stats_line.split(',')
    for stat_item in stats_list:
        try:
            pair = stat_item.split('=')
        except:
            print "Failed to get key, value pair from %s" % stats_item
        if len(pair) != 2:
            print "didn't get key and value from %s" % stats_item
            return(None)
        key = pair[0].strip(" ")
        value = pair[1].strip(" ")
        if key == 'count':
            value = int(value)
        else:
            value = float(value)
        stats[key] = value
    return(stats)

#*******************************************************************************

def show_stats(stats):
    for key,val in stats.iteritems():
        print "stats[%s] = %s" % (key, str(stats[key]))

#*******************************************************************************

def parse_file(file):
    yhat_stats = None
    lines = file.read().split('\n')
    for line in lines:
        if len(line) == 0:
            continue
        line_els = line.split(':')
        if len(line_els) != 2:
            print "didn't get metric and stats from %s" % line
            return(None, None)
        metric = line_els[0]
        stats = line_els[1]
        if metric == 'yhat':
            yhat_stats = parse_stats(stats)
        else:
            print "Failed to recognize %s" % metric
    return(yhat_stats)

#*******************************************************************************

def doMain(args):
    new = False
    yhat_stats = {}
    try:
        source = open(args.file, 'r')
    except:
        print "failed to open %s" % args.file
        return(None, None)
    yhat_file = parse_file(source)
    source.close()
    if yhat_file is None:
        print "Failed to get values from %s" % args.file
        return(None)
    if not os.access(args.add, os.R_OK):
        new = True
        yhat_stats['count'] = yhat_file['count']
        yhat_stats['sum'] = yhat_file['sum']
        yhat_stats['sumsq'] = yhat_file['sumsq']
        yhat_stats['ave'] = yhat_file['ave']
        yhat_stats['med'] = yhat_file['med']
        yhat_stats['sdev'] = yhat_file['sdev']
        yhat_stats['min'] = yhat_file['min']
        yhat_stats['max'] = yhat_file['max']
    else:
        try:
            target = open(args.add, 'r')
        except:
            print "Failed to open %s" % args.add
            return(None)
        yhat_add = parse_file(target)
        target.close()
        for field in ('count', 'sum', 'sumsq'):
            yhat_stats[field] = yhat_file[field] + yhat_add[field]
        if yhat_file['count'] <= 0:
            print "Problem with source file count"
            return(None)
        yhat_stats['ave'] = yhat_stats['sum']/yhat_stats['count']
        yhat_stats['med'] = (yhat_file['med']*yhat_file['count'] + yhat_add['med']*yhat_add['count'])/yhat_stats['count']
        yhat_stats['sdev'] = np.sqrt((yhat_stats['sumsq']/yhat_stats['count']) - (yhat_stats['ave']*yhat_stats['ave']))
        if yhat_file['min'] < yhat_add['min']:
            yhat_stats['min'] = yhat_file['min']
        else:
            yhat_stats['min'] = yhat_add['min']
        if yhat_file['max'] > yhat_add['max']:
            yhat_stats['max'] = yhat_file['max']
        else:
            yhat_stats['max'] = yhat_add['max']
    return(yhat_stats)

#*******************************************************************************
# See also ratestatsfromh5lmt.py
def doAction(args, yhat_stats):
    target = open(args.add, 'w')
    target.write(("yhat: count=%d, sum = %f, sumsq=%f, ave=%f, " +
                  "med=%f, sdev=%f, min=%f, max=%f\n") %
                 (yhat_stats['count'], yhat_stats['sum'], yhat_stats['sumsq'], yhat_stats['ave'],
                  yhat_stats['med'], yhat_stats['sdev'], yhat_stats['min'], yhat_stats['max']))
    target.close()
    return

#*******************************************************************************

if __name__ == "__main__":
    """
    accumulate_fsu_stats.py <opts>
    Options include:
    -a <file>   The report file to accumulate into
    -f <file>   The report file to read from
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        yhat_stats = doMain(args)
        if not yhat_stats is None:
            doAction(args, yhat_stats)
