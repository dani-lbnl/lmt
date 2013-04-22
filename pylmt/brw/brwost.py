#!/usr/bin/env python
# brwost.py
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

import argparse
import MySQLdb
import numpy as np
import matplotlib.pyplot as plt
import time

from pyLMT import LMTConfig, Timestamp, TimeSteps, Graph, BrwOST

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
    parser.add_argument('-a', '--all', action='store_true', default=False, help='Sum up all the values in bins for a given statistic')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-B', '--bin', default=None, type=int, help='The histogram bin to track as a time series')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-o', '--ost', default=None, type=str, help='Name of the OST to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--stat', default=None, type=str, help='The name of one of the stats to show (default: show all)')
    parser.add_argument('-S', '--showStats', action='store_true', default=False, help='Display the list of Stats rather than showing data')
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
        print "brwost.validate_args(): Please provide both a begin and an end argument (or neither for the default)"
        return(None)
    if args.ost == None:
        print "brwost: Please provide an OST"
        return(None)
    # By default do both read and write
    if (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if (not args.bin is None) and (args.stat is None):
        print "brwost.validate_args(): If you want to track a particular bin you need to specify the statistic as well. "
        return(None)
    return(args)

#*******************************************************************************
# callable main function for working interactively
def do_main(args):
    """
    """
    fsrc = LMTConfig.process_configuration(args)
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        query = "SELECT * FROM OST_INFO"
        cursor.execute (query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        cursor.close()
        print "OST: Error %d: %s" % (e.args[0], e.args[1])
        return(None)
    ost = None
    for row in rows:
        if row["OST_NAME"] == args.ost:
            BO = BrwOST.BrwOST(args.ost)
            break
    cursor.close()
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    if BO == None:
        print "OST: %s not found" % args.ost
        return(None)
    if args.verbose == True:
        BO.debug()
        #ost.debug(module="Timestamp")
    BO.setSteps(Steps)
    BO.getBrwStats(conn=fsrc['conn'], stat=args.stat)
    if args.showStats == True:
        BO.showBrwStatsNames()
        return
    BO.getData(stat=args.stat)
    return(BO)

#*******************************************************************************

def time_plot_all_bins(args, BO):
    if not args.stat in BO.BrwNameDict:
        print "brwost.py.do_action(): %s not found in the set of Statistics" % args.stat
        return
    Bins = BO.Bins[BO.BrwNameDict[args.stat]]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = BO.Steps.Steps
    times = np.diff(steps)
    ymax = 0
    bin_num = 0
    formats = ['-',  '-',  '-',   '-',   '-',   '-',    '-',    '--',   '--']
    colors  = ['b',  'g',  'r',   'c',   'm',   'y',    'k',    'b',    'r']
    labels  = ['4k', '8k', '16k', '32k', '64k', '128k', '256k', '512k', '1m']
    for bin in Bins.Bins:
        if args.read == True:
            io = "read"
            read = BO.Read[Bins.id]
            values = read.Values[Bins.BinDict[bin],:]
            values[1:] = np.diff(values)/times
            values[0] = 0.0
            max = np.max(values)
            if max > ymax:
                ymax = max
            Graph.timeSeries(ax, steps, values,
                             color=colors[bin_num],
                             label=labels[bin_num],
                             format=formats[bin_num])
        else:
            # This ends up doing write I/O by default, you have to request reads
            io = "write"
            write = BO.Write[Bins.id]
            values = write.Values[Bins.BinDict[args.bin],:]
            values[1:] = np.diff(values)/times
            values[0] = 0.0
            max = np.max(values)
            if max > ymax:
                ymax = max
            Graph.timeSeries(ax, steps, values,
                             color=colors[bin_num],
                             label=labels[bin_num],
                             format=formats[bin_num])
        bin_num += 1
    plt.xlabel('time')
    plt.ylabel(r'$count/sec$')
    plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(BO.begin.sie))
    plt.title("%s %s %s RPC %s stats" % (dayStr, BO.name, args.stat, io))
    if (not args.ybound is None) and (args.ybound > ymax):
        ymax = args.ybound
    ax.set_ybound(lower = 0, upper = ymax)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    plt.cla()
    return

#*******************************************************************************

def time_plot_bin(args, BO):
    if not args.stat in BO.BrwNameDict:
        print "brwost.py.do_action(): %s not found in the set of Statistics" % args.stat
        return
    Bins = BO.Bins[BO.BrwNameDict[args.stat]]
    if not args.bin in Bins.BinDict:
        print "brwost.py.do_action(): %s not found in the set of Bins for %s" % (args.stat, Bins.name)
        return
    read = BO.Read[Bins.id]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = BO.Steps.Steps
    times = np.diff(steps)
    ymax = 0
    values = read.Values[Bins.BinDict[args.bin],:]
    values[1:] = np.diff(values)/times
    values[0] = 0.0
    max = np.max(values)
    if max > ymax:
        ymax = max
    Graph.timeSeries(ax, steps, values, color='r', label='read', Ave=True, format='r-')
    write = BO.Write[Bins.id]
    values = write.Values[Bins.BinDict[args.bin],:]
    values[1:] = np.diff(values)/times
    values[0] = 0.0
    max = np.max(values)
    if max > ymax:
        ymax = max
    Graph.timeSeries(ax, steps, values, color='b', label='write', Ave=True, format='b-')
    plt.xlabel('time')
    plt.ylabel(r'$count/sec$')
    plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(BO.begin.sie))
    plt.title("%s %s %s bin %d RPC stats" % (dayStr, BO.name, args.stat, args.bin))
    if (not args.ybound is None) and (args.ybound > ymax):
        ymax = args.ybound
    ax.set_ybound(lower = 0, upper = ymax)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
    plt.cla()
    return

#*******************************************************************************

def time_plot_aggregate(args, BO):
    if not args.stat in BO.BrwNameDict:
        print "brwost.py.do_action(): %s not found in the set of Statistics" % args.stat
        return
    Bins = BO.Bins[BO.BrwNameDict[args.stat]]
    read = BO.Read[Bins.id]
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = BO.Steps.Steps
    times = np.diff(steps)
    ymax = 0
    values = np.sum(read.Values, axis=0)
    values[1:] = np.diff(values)/times
    values[0] = 0.0
    max = np.max(values)
    if max > ymax:
        ymax = max
    Graph.timeSeries(ax, steps, values, 'r', label='read', Ave=True, format='-')
    write = BO.Write[Bins.id]
    values = np.sum(write.Values, axis=0)
    values[1:] = np.diff(values)/times
    values[0] = 0.0
    max = np.max(values)
    if max > ymax:
        ymax = max
    Graph.timeSeries(ax, steps, values, 'b', label='write', Ave=True, format='-')
    plt.xlabel('time')
    plt.ylabel(r'$count/sec$')
    plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(BO.begin.sie))
    plt.title("%s %s %s RPC stats" % (dayStr, BO.name, args.stat))
    if (not args.ybound is None) and (args.ybound > ymax):
        ymax = args.ybound
    ax.set_ybound(lower = 0, upper = ymax)
    if args.plot is None:
        plt.show()
    else:
        plt.savefig(args.plot)
        plt.cla()
    return

#*******************************************************************************
def do_action(args, BO):
    mode = None
    if (args.read == True) and (args.write == False):
        mode = 'read'
    if (args.read == False) and (args.write == True):
        mode = 'write'
    if args.report == True:
        BO.report(mode)
    if args.plot == "noplot":
        return
    if args.all == True:
        time_plot_aggregate(args, BO)
        return

    if not args.bin is None:
        if args.bin == -1:
            time_plot_all_bins(args, BO)
        else:
            time_plot_bin(args, BO)
        return

    for Bins in BO.Bins:
        Plots = []
        if args.read == True:
            values = (BO.Read[Bins.id].Values[:,-1] -
                      BO.Read[Bins.id].Values[:,0])
            if len(values) == 1:
                print "%s bin %s: %d, all other bins were empty" % (Bins.name, Bins.Bins[0], values[0])
            elif len(values) <= 0:
                print "No values"
            else:
                Plots.append({'values': values,
                              'label':'read',
                              'color':'r'})
        if args.write == True:
            values = (BO.Write[Bins.id].Values[:,-1] -
                      BO.Write[Bins.id].Values[:,0])
            if len(values) == 1:
                print "%s bin %s: %d, all other bins were empty" % (Bins.name, Bins.Bins[0], values[0])
            elif len(values) <= 0:
                print "No values"
            else:
                Plots.append({'values': values,
                              'label':'write',
                              'color':'b'})
        if len(Plots) > 0:
            Graph.BrwOST_hist(Bins.name, Bins.Bins, Bins.units, Plots, args.plot)

#*******************************************************************************

if __name__ == "__main__":
    """
    brwost.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -o <ost>    The name of the OST to examine
    -p <file>   File name of .png file for graph
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -s <stat>   Show histograms only for the <stat> statistics
    -S          List the available statistics and exit
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph

    Rudimentary test for OST module.

    """
    args = process_args(main=True)
    if not args is None:
        BO = do_main(args)
        if not BO is None:
            do_action(args, BO)

