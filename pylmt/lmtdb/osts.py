#!/usr/bin/env python
# osts.py
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
# -B          Show the results of read and writes added together
# -c <conf>   Path to configuration file
# -e <end>    Ending time stamp
# -f <fs>     The dbname for this filesystem in the lmtrc
# -h          A help message
# -i <index>  Index of the file system entry in the the config file
# -p <path>   Path to which .png files are sent
# -r          Print a report of statistics
# -R          Show the read rates on the graph
# -v          Print debug messages
# -V          Print the version and exit
# -W          Show the write rates on the graph
# -y <ymax>   Maximum value of the y-axis
#
#   This module supports pulling data for all OSTs from the LMT DB.
# It creates a graph for each OST individually.
#
# 2012-07-10
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
#    print "osts: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from pyLMT import Bulk, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
def process_args(main=False):
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-B', '--both', action='store_true', default=False, help='Plot the sum of the read and write rates')
    parser.add_argument('-c', '--config', default='/project/projectdirs/pma/lmt/etc/lmtrc', type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-p', '--plot', default=None, type=str, help='Path to the place where the plots are saved')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if args.plot is None:
        print "osts: You must supply a destination directory: \"-p <path>\""
        sys.exit(1)
    if (args.both == True) and ((args.read == True) or (args.write == True)):
        print "osts: Try doing either -B (both) or one or both of -R (read) and -W (write)"
        sys.exit(1)
    if (args.both == False) and (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         both - Add together the read and wtites in the plot
         config - (file) The lmtrc config file telling how to get to the DB
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         report - (boolean) Print out summary info about the analyzed operations
         read - Plot read data rate
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Plot the write rate
         ybound - (float) Use the given value as the maximum of the y-acis
    """
    # do_main will:
    # - process_configuration
    # - get all the OSSs which gets their OSTs as well
    # - Process timestamps
    # - get the data
    # - return the osts object
    fsrc = LMTConfig.process_configuration(args)
    bulk = Bulk.Bulk(fsrc['name'])
    if args.verbose == True:
        bulk.debug()
    bulk.getOSSs(fsrc['conn'])
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk.setSteps(Steps)
    if (bulk.Steps is None) or (bulk.Steps.steps() == 0):
        print "osts: Warning - No steps from FS %s" % bulk.name
        sys.exit(1)
    bulk.getData()
    return(bulk)

#*******************************************************************************
def do_plots(bulk, mode=None, plot=None, ybound=None,
            scale=1024.0*1024.0):
    for oss in bulk.OSSs:
        for ost in oss.OSTs:
            fig = plt.figure()
            ax = fig.add_subplot(111)
            steps = ost.Steps.Steps
            ymax = 0
            if mode == 'Both':
                values = ost.OST.Values/scale
                max = np.max(values)
                if max > ymax:
                    ymax = max
                Graph.timeSeries(ax, steps, values, 'b',
                                 label='read and write', Ave=True)
            elif mode is None:
                values = ost.Read.Values/scale
                max = np.max(values)
                if max > ymax:
                    ymax = max
                Graph.timeSeries(ax, steps, values, 'r', label='read',
                                 Ave=True)
                values = ost.Write.Values/scale
                max = np.max(values)
                if max > ymax:
                    ymax = max
                Graph.timeSeries(ax, steps, values, 'b', label='write',
                                 Ave=True)
            elif mode == 'Read':
                values = ost.Read.Values/scale
                max = np.max(values)
                if max > ymax:
                    ymax = max
                Graph.timeSeries(ax, steps, values, 'r', label='read',
                                 Ave=True)
            else:
                values = ost.Write.Values/scale
                max = np.max(values)
                if max > ymax:
                    ymax = max
                Graph.timeSeries(ax, steps, values, 'b', label='write',
                                 Ave=True)
            plt.xlabel('time')
            plt.ylabel(r'$MiB/sec$')
            plt.legend()
            plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
            dayStr = time.strftime("%Y-%m-%d", time.localtime(ost.begin.sie))
            plt.title("%s %s %s %s aggregate I/O" % (dayStr, bulk.name, oss.name, ost.name))
            if ybound is None:
                ybound = ymax
            ax.set_ybound(lower = 0, upper = ybound)
            plt.savefig( plot + '/' + ost.name + '.png' )
            plt.cla()

#*******************************************************************************

def do_action(args, bulk):
    if args.report == True:
        bulk.report()
    if args.plot == "noplot":
        return
    if args.both == True:
        mode = 'Both'
    elif args.read == args.write:
        mode = None
    elif args.read == True:
        mode = 'Read'
    else:
        mode = 'Write'
    do_plots(bulk, mode=mode, plot=args.plot, ybound=args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    osts.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -B          Show the results of read and writes added together
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling data for all the OSSs of a given file system
    from the LMT DB.
    """
    args = process_args(main=True)
    bulk = do_main(args)
    do_action(args, bulk)

