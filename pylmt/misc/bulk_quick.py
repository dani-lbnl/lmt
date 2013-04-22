#!/usr/bin/env python
# bulk_quick.py
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
# -C          Plot the CPU utilization
# -e <end>    Ending time stamp
# -f <fs>     The dbname for this filesystem in the lmtrc
# -h          A help message
# -i <index>  Index of the file system entry in the the config file
# -m          Plot the cross correlation spectrum
# -M <mask>   Filter out samples of the spectrum based on this mask
#                <mask> is a string of key=values pairs with keys:
#                left, right, top, bottom
# -p <file>   File name of .png file for graph
# -r          Print a report of statistics
# -R          Show the read rates on the graph
# -v          Print debug messages
# -V          Print the version and exit
# -W          Show the write rates on the graph
# -x          Cross correlate with the CPU utilization info
# -y <ymax>   Maximum value of the y-axis
#
#   This module supports pulling data from the bulk I/O rates aggregate
# table in LMT: FILESYSTEM_AGGREGATE
#
# 2012-02-13
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
#    print "bulk_quick: backend = %s" % backend
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
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--spectrum', action='store_true', default=False, help='Plot the cross-correlation spectrum')
    parser.add_argument('-M', '--mask', default=None, type=str, help=r'Filter out samples of the spectrum based on the mask. eg. "left=0.0,right=0.1" only shows the values with CPU utilization up to 0.1. "bottom=0.0,top=0.1" does the same for data rate')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    parser.add_argument('-x', '--x_correlate', action='store_true', default=False, help='Scatter plot of CPU utilization vs. -o <oss>')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.both == True) and ((args.read == True) or (args.write == True)):
        print "bulk_quick: Try doing either -B (both) or one or both of -R (read) and -W (write)"
        sys.exit(1)
    if (args.both == False) and (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if args.mask != None:
        args.cpu = True
    if args.spectrum == True:
        args.x_correlate = True
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         both - Add together the read and wtites in the plot
         config - (file) The lmtrc config file telling how to get to the DB
         cpu - plot CPU utilization
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         mask - (key=value:keys in {mincpu, maxcpu, minval, maxval}) mask values outside the given range
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         report - (boolean) Print out summary info about the analyzed operations
         read - Plot read data rate
         spectrum - Plot the cross-correlation spectrum
         frac - Filter out samples of the spectrum below this fraction of the max
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Plot the write rate
         x_correlate - (boolean) plot the ops versus the CPU utilization rather than
                         the ops versus time.
         ybound - (float) Use the given value as the maximum of the y-acis
    """
    # do_main will:
    # - process_configuration
    # - get all the OSSs which gets their OSTs as well
    # - Process timestamps
    # - get the data including CPU utilization if asked
    # - return the Bulk object
    fsrc = LMTConfig.process_configuration(args)
    bulk = Bulk.Bulk(fsrc['name'])
    if args.verbose == True:
        bulk.debug()
    #bulk.getOSSs(fsrc['conn'])
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    bulk.setSteps(Steps)
    bulk.getQuickData(conn=fsrc['conn'])
    if (bulk.Steps == None) or (bulk.Steps.steps() == 0):
        print "bulk_quick: Warning - No steps from FS %s" % bulk.name
        sys.exit(1)
    return(bulk)

#*******************************************************************************
def do_plot(bulk, mode, plot, scale):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = bulk.Steps.Steps
    ymax = 0
    if mode == 'Both':
        values = bulk.Bulk.Values/(1024*1024)
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='read and write', Ave=True)
    elif mode == None:
        values = bulk.Read.Values/(1024*1024)
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'r', label='read', Ave=True)
        values = bulk.Write.Values/(1024*1024)
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    elif mode == 'Read':
        values = bulk.Read.Values/(1024*1024)
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'r', label='read', Ave=True)
    else:
        values = bulk.Write.Values/(1024*1024)
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='write', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    if bulk.CPU != None:
        values = bulk.CPU.Values
        (handles, labels) = Graph.percent(ax, steps, values, 'k', label='% CPU', Ave=True)
        if (handles != None) and (labels != None):
            plt.legend(handles, labels)
        else:
            print "bulk_quick.do_plot(): Warning - Plotting CPU utilization failed."
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s aggregate I/O" % (dayStr, bulk.name))
    if scale == None:
        scale = ymax
    ax.set_ybound(lower = 0, upper = scale)
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()

#*******************************************************************************

def do_xcorr(bulk, mode, plot, ymax=None, scale=1024*1024):
    """
    """
    if not ((mode == 'Read') or (mode == 'Write') or (mode == 'Both') or (mode == None)):
        print "bulk_quick.do_xcorr: Error - Unrecognized mode %s" % mode
        return
    if bulk.CPU == None:
        print "bulk_quick.do_xcorr(): Error - There is no CPU utilization data for %s" % bulk.name
        return
    if ((bulk.Read == None) or (bulk.Write == None) or (bulk.Bulk == None)):
        print "bulk_quick.do_xcorr(): Error - There is no data"
        return(None)
    if ymax == None:
        if mode == 'Read':
            ymax = bulk.Read.getMax()/scale
        elif mode == 'Write':
            ymax = bulk.Write.getMax()/scale
        elif mode == 'Both':
            ymax = bulk.Bulk.getMax()/scale
        else:
            readMax = bulk.Read.getMax()/scale
            writeMax = bulk.Write.getMax()/scale
            if readMax > writeMax:
                ymax = readMax
            else:
                ymax = writeMax
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode == None) or (mode == 'Read'):
        Graph.scatter(ax, bulk.CPU.Values, bulk.Read.Values/scale, 'r', label="read")
    if (mode == None) or (mode == 'Write'):
        Graph.scatter(ax, bulk.CPU.Values, bulk.Write.Values/scale, 'b', label="write")
    if mode == 'Both':
        Graph.scatter(ax, bulk.CPU.Values, bulk.Bulk.Values/scale, 'b', label="read+write")
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ymax)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s Bulk %s activity vs \%CPU" % (dayStr, bulk.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)

#*******************************************************************************

def do_spectrum(bulk, mode, plot, ybound):
    maxRate = 50.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = bulk.CPU.Values/maxCPU
    if (mode == 'Read') or (mode == None):
        rate = bulk.Read.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode == None):
        rate = bulk.Write.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Write')
    if mode == 'Both':
        rate = bulk.Bulk.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Both')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if ybound != None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(bulk.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, bulk.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot == None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************

def do_action(args, bulk):
    if args.mask != None:
        bulk.doMask(args.mask)
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
    if args.spectrum == True:
        do_spectrum(bulk, mode, args.plot, args.ybound)
        return
    if args.x_correlate == True:
        do_xcorr(bulk, mode, args.plot, ymax=args.ybound, scale=1024*1024)
        return
    do_plot(bulk, mode, args.plot, args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    bulk_quick.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -B          Show the results of read and writes added together
    -c <conf>   Path to configuration file
    -C          Plot the CPU utilization
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -m          Plot the cross correlation spectrum
    -M <mask>   Filter out samples of the spectrum based on this mask
                <mask> is a string of key=values pairs with keys:
                left, right, top, bottom
    -p <file>   File name of .png file for graph
    -P          Calculate and plot the standard deviation across OSTs at each timestep
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph
    -x          Cross correlate with the CPU utilization info
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling data for all the OSSs of a given file system
    from the LMT DB.
    """
    args = process_args(main=True)
    if not args is None:
        bulk = do_main(args)
        if not bulk is None:
            do_action(args, bulk)

