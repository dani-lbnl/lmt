#!/usr/bin/env python
# missing.py
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
# -b <begin>  Beginning time stamp
# -c <conf>   Path to configuration file
# -e <end>    Ending time stamp
# -f <fs>     The dbname for this filesystem in the lmtrc
# -h          A help message
# -i <index>  Index of the file system entry in the the config file
# -p <file>   File name of .png file for graph
# -r          Print a report of statistics
# -v          Print debug messages
# -V          Print the version and exit
# -y <ymax>   Maximum value of the y-axis
#
#   This module supports pulling data for all OSTs from the LMT DB and looking
# for missing data.
#
# 2012-11-30
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
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] is None):
#    print "bulk: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from pyLMT import Bulk, LMTConfig, Graph, Timestamp, TimeSteps

#*******************************************************************************
def process_args(main=False):
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default='/project/projectdirs/pma/lmt/etc/lmtrc', type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         config - (file) The lmtrc config file telling how to get to the DB
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         report - (boolean) Print out summary info about the analyzed operations
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
                         the ops versus time.
         ybound - (float) Use the given value as the maximum of the y-acis
    """
    # do_main will:
    # - process_configuration
    # - get all the OSSs which gets their OSTs as well
    # - Process timestamps
    # - get the data including CPU utilization if asked
    # - return the bulk object
    fsrc = LMTConfig.process_configuration(args)
    if fsrc is None:
        return None
    bulk = Bulk.Bulk(fsrc['name'])
    if args.verbose == True:
        bulk.debug()
    bulk.getOSSs(fsrc['conn'])
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk.setSteps(Steps)
    if (bulk.Steps is None) or (bulk.Steps.steps() == 0):
        print "bulk: Warning - No steps from FS %s" % bulk.name
        sys.exit(1)
    bulk.getCPU()
    return(bulk)

#*******************************************************************************
def do_plot(bulk, plot=None, ybound=30):
    steps = bulk.Steps.Steps
    num_steps = len(steps)
    num_osss = bulk.getNumOSSs()
    Missing = np.zeros(num_steps)
    for oss in bulk.OSSs:
        Missing += oss.Missing
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, steps, Missing, 'r', label='missing',
                     Ave=False, format='+')
    plt.xlabel('time')
    plt.ylabel(r'$count$')
    plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s Missing Observations" % (dayStr, bulk.name))
    ax.set_ybound(lower = 0, upper = ybound)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()

#*******************************************************************************

def do_action(args, bulk):
    if args.report == True:
        bulk.report()
    if args.plot == "noplot":
        return
    do_plot(bulk, plot=args.plot, ybound=args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    bulk.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -v          Print debug messages
    -V          Print the version and exit
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling data for all the OSSs of a given file system
    from the LMT DB.
    """
    args = process_args(main=True)
    if not args is None:
        bulk = do_main(args)
        if not bulk is None:
            do_action(args, bulk)

