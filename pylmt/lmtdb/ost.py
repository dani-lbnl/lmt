#!/usr/bin/env python
# ost.py
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
# -l          Graph using lines (no matter how many data points)
# -o <ost>    The name of the OST to examine
# -p <file>   File name of .png file for graph
# -r          Print a report of statistics
# -R          Show the read rates on the graph
# -s          Show the list of OSSs
# -v          Print debug messages
# -V          Print the version and exit
# -W          Show the write rates on the graph
# -y <ymax>   Maximum value of the y-axis
#
#   This module supports pulling OST data from the LMT DB.
#
# 2011-10-20
# - version 0.1
#
# 2011-12-02
# - Allow for multiple -o <ost> arguments
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
#    print "ost: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from pyLMT import OST, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    The command line arguments needed for operating the OST class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-B', '--both', action='store_true', default=False, help='Plot the sum of the read and write rates')
    parser.add_argument('-c', '--config', default='/project/projectdirs/pma/lmt/etc/lmtrc', type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-l', '--lines', action='store_true', default=False, help='Graph using lines (no matter how many data points)')
    parser.add_argument('-o', '--ost', default=None, type=str, help='Name of the OST to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--show_osts', action='store_true', default=False, help='Print the list of OSTs in the DB')
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
    if (((args.begin == None) and (args.end != None)) or
        ((args.begin != None) and (args.end == None))):
        print "ost.validate_args(): Please provide both a begin and an end argument (or neither for the default)"
        return(None)
    if (args.ost == None) and (args.show_osts == False):
        print "ost: Please provide an OST, or use -s to list them"
        return(None)
    if (args.both == True) and ((args.read == True) or (args.write == True)):
        print "ost: Try doing either -B (both) or one or both of -R (read) and -W (write)"
        return(None)
    # By default do both read and write
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    return(args)

#*******************************************************************************
# callable main function for working interactively
def do_main(args):
    """
    """
    fsrc = LMTConfig.process_configuration(args)
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        query = "SELECT * FROM OST_INFO"
        cursor.execute (query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        cursor.close()
        print "ost: Error %d: %s" % (e.args[0], e.args[1])
        sys.exit(1)
    ost = None
    for row in rows:
        if args.show_osts == True:
            print "%s" % row["OST_NAME"]
        else:
            if row["OST_NAME"] == args.ost:
                ost = OST.OST(args.ost)
                break
    cursor.close()
    if args.show_osts == True:
        sys.exit(0)
    if ost == None:
        print "ost: %s not found (try -s)" % args.ost
        sys.exit(0)
    if args.verbose == True:
        ost.debug()
        #ost.debug(module="Timestamp")
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    ost.setSteps(Steps)
    ost.getData(fsrc['conn'])
    return(ost)

#*******************************************************************************
def do_plot(name, steps, Plots, plot, ybound):
    if args.lines == True:
        format = '-'
    else:
        format = None
    fig = plt.figure()
    ax = fig.add_subplot(111)
    for Plot in Plots:
        Graph.timeSeries(ax, steps, Plot['values'], Plot['color'],
                         Plot['label'], Ave=True, format=format)
    plt.setp( ax.get_xticklabels(), rotation=30,
              horizontalalignment='right')
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    plt.legend()
    plt.title("%s I/O" % (name))
    ax.set_ybound(lower=0, upper=ybound)
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()

#*******************************************************************************
def do_action(args, ost):
    if args.report == True:
        ost.report()
    if args.plot == "noplot":
        return
    scale=1024*1024
    Plots = []
    ymax = 0
    if args.both == True:
        values = ost.OST.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Plots.append({'values': values,
                      'label':'read and write',
                      'color':'b'})
    if args.read == True:
        values = ost.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Plots.append({'values': values,
                      'label':'read',
                      'color':'r'})
    if args.write == True:
        values = ost.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Plots.append({'values': values,
                      'label':'write',
                      'color':'b'})
    if args.ybound == None:
        args.ybound = ymax
    do_plot(ost.name, ost.Steps.Steps, Plots, args.plot, args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    ost.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -B          Show the results of read and writes added together
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -l          Graph using lines (no matter how many data points)
    -o <ost>    The name of the OST to examine
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -s          Show the list of OSSs
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling OST data from the LMT DB.
    """
    args = process_args(main=True)
    try:
        ost = do_main(args)
        if ost != None:
            do_action(args, ost)
    except AttributeError:
        pass

