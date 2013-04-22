#!/usr/bin/env python
# missingfromh5lmt.py.py
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
# -e <end>    Ending time stamp
# -f <file>   The h5lmt file to query
# -h          A help message
# -p <plot>   File name of .png file for graph
# -v          Print debug messages
# -V          Print the version and exit
#
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
#    print "missingfromh5lmt: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
import h5py

from pyLMT import Graph, Timestamp

#*******************************************************************************
def process_args(main=False):
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--file', default=None, type=str, help='The h5lmt file to query')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if args.file is None:
        print "Please provide a file"
        return(None)
    if args.begin is None:
        print "Please provide a begin and end timestamp in the form 'yyyy-mm-dd hh-mm-ss'"
        return(None)
    if args.end is None:
        print "Please provide an end timestamp in the form 'yyyy-mm-dd hh-mm-ss' as well"
        return(None)
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         end - (string) As above giving the end of the data to be gathered.
         file - (string) The h5lmt file to query
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
                         the ops versus time.
    """
    fsFile = h5py.File(args.file, 'r')
    b_sie = Timestamp.calc_sie(args.begin)
    e_sie = Timestamp.calc_sie(args.end)
    return(b_sie, e_sie, fsFile)

#*******************************************************************************

def find_sie(sie, dataSet):
    first = 0
    last = len(dataSet) - 1
    while first < last:
        mid = int((first+last)/2)
        if sie == dataSet[mid]:
            return(mid)
        if first == last -1:
            if ((dataSet[first] <= sie) and
                (dataSet[last] > sie)):
                return(first)
            if sie == dataSet[last]:
                return(last)
            print "brw_stats_model_h5lmt.find_sie(): Binary seach for %d failed at (%d, %d). Are there outof order timestamp entries?" % (sie, first, last)
            return(None)
        if sie < dataSet[mid]:
            last = mid
        else:
            first = mid

#*******************************************************************************

def doAction(args, b_sie, e_sie, fsFile):
    fsStepsGroup = fsFile['FSStepsGroup']
    fsStepsDataSet = fsStepsGroup['FSStepsDataSet']
    if (b_sie < fsStepsDataSet[0]) or (b_sie > fsStepsDataSet[-1]):
        print "The beginning timestamp %d is outside the date range from %d to %d" % (b_sie, fsStepsDataSet[0], fsStepsDataSet[-1])
        return
    if (e_sie < fsStepsDataSet[0]) or (e_sie > fsStepsDataSet[-1]):
        print "The beginning timestamp %d is outside the date range from %d to %d" % (e_sie, fsStepsDataSet[0], fsStepsDataSet[-1])
        return
    b_index = find_sie(b_sie, fsStepsDataSet)
    e_index = find_sie(e_sie, fsStepsDataSet)
    #print b_index, e_index
    fs=fsStepsDataSet.attrs['fs']
    try:
        host=fsStepsDataSet.attrs['host']
    except:
        host='hopper'
    ossCPUGroup = fsFile["OSSCPUGroup"]
    ossCPUDataSet = ossCPUGroup["OSSCPUDataSet"]
    fsMissingGroup = fsFile["FSMissingGroup"]
    fsMissingDataSet = fsMissingGroup["FSMissingDataSet"]
    num_steps = e_index - b_index + 1
    Missing = np.zeros(num_steps)
    oss_index = 0
    for oss_name in ossCPUDataSet.attrs["OSSNames"]:
        Missing += fsMissingDataSet[oss_index,b_index:e_index+1]
        oss_index += 1
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.timeSeries(ax, fsStepsDataSet[b_index:e_index+1], Missing, 'r',
                     label='missing', Ave=False, format='+')
    plt.xlabel('time')
    plt.ylabel(r'$count$')
    plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(b_sie))
    plt.title("%s %s %s Missing Observations" % (dayStr, host, fs))
    ax.set_ybound(lower = 0, upper = 30)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    plt.cla()

#*******************************************************************************

if __name__ == "__main__":
    """
    missingfromh5lmt.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -e <end>    Ending time stamp
    -f <file>   The h5lmt file to query
    -h          A help message
    -p <file>   File name of .png file for graph
    -v          Print debug messages
    -V          Print the version and exit

    """
    args = process_args(main=True)
    if not args is None:
        b_sie, e_sie, fsFile = do_main(args)
        if not ((fsFile is None) or (b_sie is None) or (e_sie is None)):
            doAction(args, b_sie, e_sie, fsFile)
            fsFile.close()

