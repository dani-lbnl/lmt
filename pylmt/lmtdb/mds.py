#!/usr/bin/env python
# mds.py <opts>
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
# -b <begin>  Beginning time stamp
# -c <conf>   Path to configuration file
# -C          Plot the CPU utilization
# -e <end>    Ending time stamp
# -E <extra>  Provide <extra> percent in the graph before and after the
#               specified interval, but don't include in average calculation.
# -f <fs>     The dbname for this filesystem in the lmtrc
# -h          A help message
# -H <op>     In a -x cross-correlation graph, show the portion of CPU
#               accounted for by <op> (default: 'open')
# -i <index>  Index of the file system entry in the the config file
# -m          Plot the cross correlation spectrum
# -M <mask>   Filter out samples based on this mask
#                <mask> is a string of key=values pairs with keys:
#                mincpu, maxcpu, minval, maxval
# -p <file>   file name of .png file for graph
# -r          Produce a report on the observed values
# -s          Show the list of operations
# -v          Print debug messages
# -V          Print the version and exit
# -x          Cross correlate with the CPU utilization info
# -X          Show the steps for values in the given bounding box (-M <mask>)
# -y <ymax>   Maximum value of the y-axis
#
# 2012-08-20
# - version 0.1
# - Extract MDS.py invokation script
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
#    print "mds: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import Axes3D
from pyLMT import MDS, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
# Support for basic calling conventions
def process_args(main=False):
    """
    The command line arguments needed for operating the MDS class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-c', '--config', default='/project/projectdirs/pma/lmt/etc/lmtrc', type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-E', '--extra', default=None, type=int, help='Extra graph before/after interval (default no padding)')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-H', '--hilite', default=None, type=str, help='In a -x cross-correlation graph, show the portion of CPU accounted for by <op> (default: "open")')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--spectrum', action='store_true', default=False, help='Plot the cross-correlation spectrum')
    parser.add_argument('-M', '--mask', default=None, type=str, help='Filter out samples based on the mask. eg. "left=0.0,right=0.1" only shows the values with CPU utilization up  to 0.10. "bottom=0.0,top=0.1" does the same for data rate')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Produce a report on the observed values')
    parser.add_argument('-s', '--show_ops', action='store_true', default=False, help='Print the list of operations')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-x', '--x_correlate', action='store_true', default=False, help='Scatter plot of CPU utilization vs. -m <op>')
    parser.add_argument('-X', '--show_steps', action='store_true', default=False, help='Show the steps in a bounding box (-M <mask>)')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.show_steps == True) or (not args.mask is None):
        args.cpu = True
    if args.spectrum == True:
        args.x_correlate = True
    return(args)

#*******************************************************************************
# callable main function for working interactively
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         config - (file) The lmtrc config file telling how to get to the DB
         cpu - plot CPU utilization
         end - (string) As above giving the end of the data to be gathered.
         extra - (int) If None then ignore, otherwise pad the graph before and after
                 after the interval with this much extra, but don't inlcude in
                 average calculation.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         mask - (key=value:keys in {mincpu, maxcpu, minval, maxval}) mask values outside the given range
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         report - (boolean) Print out summary info about the analyzed operations
         show_ops - (boolean) List the available operations
         show_steps - (boolean) List the steps that are in the -M <mask> region
         spectrum - (boolean)  Produce a graph of the Ops rate versus CPU utilization spectrum
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         x_correlate - (boolean) plot the ops versus the CPU utilization rather than
                         the ops versus time.
         ybound - (float) Use the given value as the maximum of the y-acis
    """
    fsrc = LMTConfig.process_configuration(args)
    mds = MDS.MDS(host=fsrc['host'], fs=fsrc['name'])
    if args.verbose == True:
        mds.debug()
        mds.debug(module="Operation")
        #mds.debug(module="Timestamp")
    mds.opsFromDB(fsrc['conn'])
    if args.show_ops == True:
        mds.showOps()
        return
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    mds.setSteps(Steps)
    mds.getData()
    if mds.haveData == False:
        print "got no data from %s to %s" % (args.begin, args.end)
        return(None)
    if (args.cpu == True) or (args.x_correlate == True):
        mds.getCPU()
    if not args.extra is None:
        if (args.extra <= 0) or (args.extra >= 100):
            print "mds: extra should be an integer between 0 and 100"
            args.extra = None
    return(mds)

#*******************************************************************************
def do_plot(mds, plot, ymax, extra=None):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = mds.Steps.Steps
    values = mds.MDS.Values
    if ymax is None:
        ymax = np.max(values)
    Graph.timeSeries(ax, steps, values, 'b', label='MDS', Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$ops/sec$')
    if not mds.CPU is None:
        values = mds.CPU.Values
        (handles, labels) = Graph.percent(ax, steps, values, 'k',
                                          label='% CPU', Ave=True)
        if (not handles is None) and (not labels is None):
            plt.legend(handles, labels)
        else:
            print "mds.do_plot(): Warning - Plotting CPU utilization failed."
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    start_time = steps[0]/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    plt.title("%s metadata operations for %s" %
              (mds.name,
               mpl.dates.num2date(start_time).strftime("%Y-%m-%d"))
              )
    if ymax is None:
        ymax = ymax
    ax.set_ybound(lower=0, upper=ymax)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()

#*******************************************************************************
def do_xcorr(mds, plot=None, ybound=None, hilite="open"):
    if mds.CPU is None:
        print "mds.do_xcorr(): Error - There is no CPU utilization data for %s" % mds.name
        return
    if mds.MDS is None:
        print "mds.do_xcorr(): Error - There is no data for %s" % mds.name
        return
    if ybound is None:
        ybound = mds.MDS.getMax()
    fig = plt.figure()
    ax = fig.add_subplot(111)
    Graph.scatter(ax, mds.CPU.Values, mds.MDS.Values, 'b', label="mds ops")
    if not hilite is None:
        n = 0
        op_index = None
        for op in mds.Ops:
            #print "mds.do_xcorr(): Debug - op name = %s, hilite = %s" % (op.name, hilite)
            if ((op.Values is None) or (op.Steps is None) or
                (op.Steps.steps() == 0) or (op.Stats is None)):
                continue
            if op.name == hilite:
                op_index = n
            n += 1
        if n > 0:
            # mds.x is teh result of a generalized linear regression apporioning
            # the fraction of CPU utilization to each operation. x[n] is the
            # final category, of "no operation".
            if mds.x is None:
                mds.attribute()
            if (not op_index is None) and (mds.x[op_index] != 0.0):
                slope = (100 - mds.x[n])/(100*mds.x[op_index])
                model_x = np.array(range(101))
                model_y = mds.x[n]/100.0 + slope*model_x
                #print "hilite = %s, op_index = %d, y(0) = %f, y(1) = %f" % (hilite, op_index, model_y[0], model_y[99])
                ax.plot(model_x, model_y, "r-", label=hilite)
        else:
            print "mds.do_xcorr(): Warning - No ops with data for regression"
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    ax.legend()
    plt.title("%s activity vs %%CPU" % mds.name)
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$ops/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()

#*******************************************************************************
def do_spectrum(mds, plot, ybound=None):
    maxRate = 125000.0
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = mds.CPU.Values/maxCPU
    rate = mds.MDS.Values/maxRate
    ratio = np.zeros_like(rate)
    ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
    spectrum = np.arctan(ratio)
    hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
    ax.plot(bins[1:-1], hist[1:], 'b-', label='MDS ops')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(mds.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, mds.name))
    ax.set_xlabel(r"arctan($((Ops/s)/125000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************
def do_action(args, mds):
    """
    This just implements the basic response of mds.py if called as an application
    rather than a library. It will print out a report if asked. It will produce
    a scatter plot of ops versus CPU utilization if asked. If not a scatter plot
    and if grphin hasn't been suppressed it will produce a graph of the ops
    over time and either display it or save it to the indicated file.
    """
    if args.show_ops == True:
        # We don't anticipate actually doing anything if we're just trying
        # to see what the available ops are.
        return
    if not args.mask is None:
        mds.doMask(args.mask)
    if args.report == True:
        mds.report()
    if args.plot == "noplot":
        return
    if args.spectrum == True:
        do_spectrum(mds, args.plot, args.ybound)
        return
    if args.x_correlate == True:
        do_xcorr(mds, plot=args.plot, ybound=args.ybound, hilite=args.hilite)
        return
    do_plot(mds, args.plot, args.ybound)

#*******************************************************************************


if __name__ == "__main__":
    """
    mds.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -C          Plot the CPU utilization
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -H <op>     In a -x cross-correlation graph, show the portion of CPU
                  accounted for by <op> (default: 'open')
    -i <index>  Index of the file system entry in the the config file
    -M <mask>   Filter out samples based on this mask
                <mask> is a string of key=values pairs with keys:
                left, right, top, bottom
    -p <file>   file name of .png file for graph
    -r          Produce a report on the observed values
    -s          Plot the cross-correlation spectrum
    -v          Print debug messages
    -V          Print the version and exit
    -x          Cross correlate with the CPU utilization info
    -y <ymax>   Maximum value of the y-axis

    This module supprts pulling MDS data from the LMT DB.

    2011-10-20
    - version 0.1
    2011-12-15
    - version 0.2

    """
    args = process_args(main=True)
    mds = do_main(args)
    if (mds is None) or (mds.haveData == False):
        print "mds: No data"
        sys.exit()
    do_action(args, mds)
