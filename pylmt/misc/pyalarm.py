#!/usr/bin/env python
# data_moved.py
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
#
# Options include:
# -B         Show the results of read and writes added together
# -c <conf>  Path to configuration file
# -C          Plot the CPU utilization
# -f <fs>    The dbname for this filesystem in the lmtrc
# -h         A help message
# -i <index> Index of the file system entry in the the config file
# -o <oss>   The name of the OSS to examine
# -s <sec>   Check the average value over the last <sec> seconds
#            (default 60)
# -t <thrsh> Sound the alarm if the average exceeds this value
#            (40 GiB/s for rates, 40% for CPU utilization)
# -R         Show the read rates on the graph
# -v         Print debug messages
# -V         Print the version and exit
# -W         Show the write rates on the graph
#
#    This module supports pulling data for a given day from the LMT DB and
# checking to see if a given condition is met. It will exit(0) when the
# condition is met and exit(1) otherwise.
#
# 2012-01-31
# - version 0.1
#
# Todo:
# - Update this todo list :)

import sys
import os
import re
import time
import string
import argparse
import datetime
import traceback
import MySQLdb
import numpy as np
import numpy.ma as ma
import matplotlib as mpl
# If this is run from a cron job it is not a login process and
# has no X $DISPLAY in the environment. You can prevent pyplot
# from getting confused by telling matplotlib to use the 'Agg'
# backend. On the other hand, if you've already loaded pyplot
# or pylab then it's too late to use the mpl.use('Agg') and would
# generate an obnoxious warning if you try. N.B. The backend
# property is not case sensitive and get_backend() actually
# returns a lower case string.
backend = mpl.get_backend()
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] == None):
#    print "data_moved: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from pyLMT import Bulk, LMTConfig, Timestamp, TimeSteps

#*******************************************************************************
def process_args(main=False):
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-B', '--both', action='store_true', default=False, help='Plot the sum of the read and write rates')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-o', '--oss', default=None, type=str, help='Name of the OSS to examine')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--seconds', default=60, type=int, help='Examine the I/O over this many recent seconds (default 60)')
    parser.add_argument('-t', '--threshold', default=40.0, type=float, help='Sound the alarm if the value exceeds this threshold (default - 40: GiB for rates or pct CPU utilization)')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    # By default do read
    args.fail = False
    count = 0
    if args.both == True:
        count += 1
    if args.cpu == True:
        count += 1
    if args.read == True:
        count += 1
    if args.write == True:
        count += 1
    if count > 1:
        print "pyalarm.validate_args(): Select only one of -B, -C, -R, or -W"
        args.fail = True
        return(args)
    if count == 0:
        args.read = True
    if args.seconds <= 0:
        print "pyalarm.validate_args(): please provide a positive number of seconds"
        args.fail = True
        return(args)
    if args.threshold <= 0:
        print "pyalarm.validate_args(): please provide a positive number threshold (GiB/\%)"
        args.fail = True
        return(args)
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.both - Add together the read and write rates for calculation
         config - (file) The lmtrc config file telling how to get to the DB
         cpu - (boolean) Do the CPU utilization calculations (always set this true)
         fs - (string) The dbname entry in the config file for the file system of
                       interest.
         index - (int) The index of the file system of interest in the config file
         oss - (string) The hostname of the OSS to be analyzed.
         read - Caclulate for the read data rate
         seconds - (int) The length of time over which to average the observations.
         threshold - (int) Sound the alarm if the average exceeds this value.
                           default - 40: GiB for rates, or pct. CPU utilization
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Calculate for the write data rate
    """
    fsrc = LMTConfig.process_configuration(args)
    bulk = Bulk.Bulk(fsrc['dbname'])
    if args.verbose == True:
        bulk.debug()
    bulk.getOSSs(fsrc['conn'])
    now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    # Now we want to generate the begin/end pair for this day
    end_sie = Timestamp.calc_sie(now)
    # This will be upset by savings time changes
    begin_sie = end_sie - args.seconds
    args.begin = Timestamp.format_timestamp(begin_sie)
    args.end = Timestamp.format_timestamp(end_sie)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk.setSteps(Steps)
    if (bulk.Steps == None) or (bulk.Steps.steps() == 0):
        print "pyalarm.do_main(): Warning - No steps from FS %s" % bulk.name
        return(None)
    bulk.getData()
    if args.cpu == True:
        bulk.getCPU()
    return(bulk)

#*******************************************************************************
def do_action(args, bulk):
    """
    """
    oss = None
    scale = 1024.0*1024.0*1024.0
    scale_str = "GiB"
    average = 0
    if args.oss != None:
        if not args.oss in bulk.OSSDict:
            print "pyalarm.do_action(): Warning - OST %s not found" % args.ost
            return(False)
        oss = bulk.OSSs[bulk.OSSDict[args.oss]]
        if args.both == True:
            average = np.sum(oss.Both.Values)/(args.seconds*scale)
        if args.read == True:
            average = np.sum(oss.Read.Values)/(args.seconds*scale)
        if args.write == True:
            average = np.sum(oss.Write.Values)/(args.seconds*scale)
        return(average > args.threshold)
    if args.both == True:
        average = np.sum(bulk.Both.Values)/(args.seconds*scale)
    if args.read == True:
        average = np.sum(bulk.Read.Values)/(args.seconds*scale)
    if args.write == True:
        average = np.sum(bulk.Write.Values)/(args.seconds*scale)
    return(average > args.threshold)

#*******************************************************************************

if __name__ == "__main__":
    """
    data_moved.py <opts>
    Options include:
    -B         Show the results of read and writes added together
    -c <conf>  Path to configuration file
    -C         Plot the CPU utilization
    -f <fs>    The dbname for this filesystem in the lmtrc
    -h         A help message
    -i <index> Index of the file system entry in the the config file
    -o <oss>   The name of the OSS to examine
    -R         Show the read rates on the graph
    -s <sec>   Check the average value over the last <sec> seconds
                 (default 60)
    -t <thrsh> Sound the alarm if the average exceeds this value
                 (40 GiB/s for rates, 0.40 for CPU utilization)
    -v         Print debug messages
    -V         Print the version and exit
    -W         Show the write rates on the graph

    This module supports pulling data for a given day from the LMT DB and
    producing various graphs and reports. It will drop all these in a directory
    below the current directorynamed for the -d <day> value.
    """
    result = False
    args = process_args(main=True)
    if args.fail != True:
        bulk = do_main(args)
    if bulk != None:
        result = do_action(args, bulk)
    if result == True:
        sys.exit(0)
    else:
        sys.exit(1)
