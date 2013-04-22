#!/usr/bin/env python
# oss.py
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
# -a <step>   Display the apportionment of I/O among the OSTs at step '-a <step>'
# -b <begin>  Beginning time stamp
# -B          Show the results of read and writes added together
# -c <conf>   Path to configuration file
# -C          Plot the CPU utilization
# -e <end>    Ending time stamp
# -f <fs>     The dbname for this filesystem in the lmtrc
# -h          A help message
# -i <index>  Index of the file system entry in the the config file
# -l          Graph using lines (no matter how many data points)
# -m          Plot the cross correlation spectrum
# -M <mask>   Filter out samples based on this mask
#                <mask> is a string of key=values pairs with keys:
#                mincpu, maxcpu, minval, maxval
# -o <oss>    The name of the OSS to examine
# -p <file>   File name of .png file for graph
# -P          Calculate and plot the standard deviation across OSTs at each timestep
# -r          Print a report of statistics
# -R          Show the read rates on the graph
# -s          Show the list of OSSs
# -S          Show the OSTs on <oss>
# -v          Print debug messages
# -V          Print the version and exit
# -W          Show the write rates on the graph
# -x          Cross correlate with the CPU utilization info
# -X          Show the steps for values in the given bounding box (-M <mask>)
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
if (not 'DISPLAY' in os.environ) or (os.environ['DISPLAY'] is None):
#    print "oss: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from pyLMT import OSS, LMTConfig, Timestamp, TimeSteps, Graph

#*******************************************************************************
def process_args(main=False):
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-a', '--apportion', default=None, type=int, help='Display the apportionment of I/O among the OSTs at step -a <step>')
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning of the time interval to be queried in seconds in epoch (default "ten minutes ago")')
    parser.add_argument('-B', '--both', action='store_true', default=False, help='Plot the sum of the read and write rates')
    parser.add_argument('-c', '--config', default='/project/projectdirs/pma/lmt/etc/lmtrc', type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-l', '--lines', action='store_true', default=False, help='Graph using lines (no matter how many data points)')
    parser.add_argument('-m', '--spectrum', action='store_true', default=False, help='Plot the cross-correlation spectrum')
    parser.add_argument('-M', '--mask', default=None, type=str, help='Filter out samples based on the mask. eg. "mincpu=0.0,maxcpu=0.1" only shows the values with CPU utilization up  to 10%%. "minval=0.0,maxval=0.1" does the same for data rate')
    parser.add_argument('-o', '--oss', default=None, type=str, help='Name of the OSS to examine')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-P', '--plotSdevs', action='store_true', default=False, help='Calculate and plot the standard deviation across OSTs at each timestep')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Summariaze stats in a table')
    parser.add_argument('-R', '--read', action='store_true', default=False, help='Plot the read rate')
    parser.add_argument('-s', '--show_osss', action='store_true', default=False, help='Print the list of OSSs in the DB')
    parser.add_argument('-S', '--show_osts', action='store_true', default=False, help='Print the list of OSTs on -o <oss>')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-W', '--write', action='store_true', default=False, help='Plot the write rate')
    parser.add_argument('-x', '--x_correlate', action='store_true', default=False, help='Scatter plot of CPU utilization vs. -o <oss>')
    parser.add_argument('-X', '--show_steps', action='store_true', default=False, help='Show the steps in a bounding box (-M <mask>)')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if (args.oss is None) and (args.show_osss == False):
        print "oss: Please provide an OSS, or use -s to list them"
        sys.exit(1)
    if (args.both == True) and ((args.read == True) or (args.write == True)):
        print "oss: Try doing either -B (both) or one or both of -R (read) and -W (write)"
        sys.exit(1)
    if (args.both == False) and (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if (args.show_steps == True) or (not args.mask is None):
        args.cpu = True
    if args.spectrum == True:
        args.x_correlate = True
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.apportion - A seconds in epoch value at which to show the amount of
                     I/O from each OST.
         begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         both - Add together the read and writes in the plot
         config - (file) The lmtrc config file telling how to get to the DB
         cpu - plot CPU utilization
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         lines - (boolean) Graph using lines (no matter how many data points)
         mask - (key=value:keys in {mincpu, maxcpu, minval, maxval}) mask values outside the given range
         oss - (string) The hostname of the OSS to be analyzed.
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         plotSdevs (boolean) Calculate and plot the standard deviation across OSTs at each timestep
         report - (boolean) Print out summary info about the analyzed operations
         read - Plot read data rate
         show_osss - (boolean) Just list the available OSS host names to choose
                       from for the -o <oss> argument, then exit.
         show_osts - (boolean) Just list the OSTs on the OSS -o <oss>
         show_steps - (boolean) List the steps that are in the -M <mask> region
         spectrum - (boolean) Produce a graph of the rate versus CPU utilization spectrum
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
         write - Plot the write rate
         x_correlate - (boolean) plot the ops versus the CPU utilization rather than
                         the ops versus time.
         ybound - (float) Use the given value as the maximum of the y-acis
    """
    # do_main will:
    # - process_configuration
    # - get the oss in question with early terminiation if it's just a show_osss
    # - get the OSTs on it with early termination if it's just a show_osts
    # - Process timestamps
    # - get the data including CPU utilization if asked
    # - return the oss
    fsrc = LMTConfig.process_configuration(args)
    try:
        cursor = fsrc['conn'].cursor(MySQLdb.cursors.DictCursor)
        query = "SELECT * FROM OSS_INFO"
        cursor.execute (query)
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        cursor.close()
        print "oss: Error %d: %s" % (e.args[0], e.args[1])
        return
    oss = None
    for row in rows:
        if args.show_osss == True:
            print "%s" % row["HOSTNAME"]
        else:
            if row["HOSTNAME"] == args.oss:
                oss = OSS.OSS(fsrc['name'], args.oss)
                break
    cursor.close()
    if args.show_osss == True:
        return
    if oss is None:
        print "oss: %s not found (try -s)" % args.oss
        return
    if args.verbose == True:
        oss.debug()
        oss.debug(module="OST")
        #oss.debug(module="Timestamp")
    oss.getOSTs(fsrc['conn'])
    if args.show_osts == True:
        oss.showOSTs()
        return
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    if (args.cpu == True) or (args.x_correlate == True):
        oss.getCPU()
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    oss.setSteps(Steps)
    oss.getData()
    if (oss.Steps is None) or (oss.Steps.steps() == 0):
        print "oss: Warning - No steps from OSS %s" % oss.name
        return
    return(oss)

#*******************************************************************************
def do_plot(oss, mode=None, plot=None, ybound=None,
            scale=1024.0*1024.0):
    if args.lines == True:
        format = '-'
    else:
        format = None
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = oss.Steps.Steps
    ymax = 0
    if mode == 'Both':
        values = oss.OSS.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='read and write',
                         Ave=True, format=format)
    elif mode is None:
        values = oss.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'r', label='read',
                         Ave=True, format=format)
        values = oss.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='write',
                         Ave=True, format=format)
    elif mode == 'Read':
        values = oss.Read.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'r', label='read',
                         Ave=True, format=format)
    else:
        values = oss.Write.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='write',
                         Ave=True)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    if not oss.CPU is None:
        values = oss.CPU.Values
        (handles, labels) = Graph.percent(ax, steps, values, 'k',
                                          label='% CPU', Ave=True)
        if (not handles is None) and (not labels is None):
            plt.legend(handles, labels)
        else:
            print "oss.do_plot(): Warning - Plotting CPU utilization failed."
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s %s aggregate I/O" % (dayStr, oss.name))
    if ybound is None:
        ybound = ymax
    ax.set_ybound(lower = 0, upper = ybound)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()

#*******************************************************************************

def do_xcorr(oss, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    if not ((mode == 'Read') or (mode == 'Write') or (mode == 'Both') or (mode is None)):
        print "oss.do_xcorr: Error - Unrecognized mode %s" % mode
        return
    if oss.CPU is None:
        print "oss.do_xcorr(): Error - There is no CPU utilization data for %s" % oss.name
        return
    if ((oss.Read is None) or (oss.Write is None) or (oss.OSS is None)):
        print "oss.do_xcorr(): Error - There is no data"
        return(None)
    if ybound is None:
        if mode == 'Read':
            ymax = oss.Read.getMax()/scale
        elif mode == 'Write':
            ymax = oss.Write.getMax()/scale
        elif mode == 'Both':
            ymax = oss.OSS.getMax()/scale
        else:
            readMax = oss.Read.getMax()/scale
            writeMax = oss.Write.getMax()/scale
            if readMax > writeMax:
                ymax = readMax
            else:
                ymax = writeMax
        ybound = ymax
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode is None) or (mode == 'Read'):
        Graph.scatter(ax, oss.CPU.Values, oss.Read.Values/scale, 'r', label="read")
    if (mode is None) or (mode == 'Write'):
        Graph.scatter(ax, oss.CPU.Values, oss.Write.Values/scale, 'b', label="write")
    if mode == 'Both':
        Graph.scatter(ax, oss.CPU.Values, oss.OSS.Values/scale, 'b', label="read+write")
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s OSS %s activity vs %%CPU" % (dayStr, oss.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)

#*******************************************************************************
def do_sdevs(oss, mode, plot, ymax=None, scale=1024.0*1024.0):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode is None) or (mode == 'Read'):
        (ave, sdev) = oss.CalcSdev('Read', scale=scale)
        if (not ave is None) and (not sdev is None):
            coefOfVar = np.zeros_like(sdev)
            coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
            ax.plot(coefOfVar, ave, ',r', label='read', zorder=0)
    if (mode is None) or (mode == 'Write'):
        (ave, sdev) = oss.CalcSdev('Write', scale=scale)
        if (not ave is None) and (not sdev is None):
            coefOfVar = np.zeros_like(sdev)
            coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
            ax.plot(coefOfVar, ave, ',b', label='write', zorder=0)
    if mode == 'Both':
        (ave, sdev) = oss.CalcSdev('Both', scale=scale)
        if (not ave is None) and (not sdev is None):
            coefOfVar = np.zeros_like(sdev)
            coefOfVar[ave != 0.0] = sdev[ave != 0.0]/ave[ave != 0.0]
            ax.plot(coefOfVar, ave, ',b', label='read + write', zorder=0)
    if not ymax is None:
        ax.set_ybound(lower = 0, upper = ymax)
    l = plt.axvline(x=np.sqrt(5.0), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{5}$', xy=(np.sqrt(5.0), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(2.0), color='k', linestyle='--')
    (bottom, top) = ax.get_ybound()
    ax.annotate(r'$\sqrt{2}$', xy=(np.sqrt(2.0), 0.99*top),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(1.0), color='k', linestyle='--')
    l = plt.axvline(x=np.sqrt(0.5), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{1/2}$', xy=(np.sqrt(0.5), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(0.2), color='k', linestyle='--')
    ax.annotate(r'$\sqrt{1/5}$', xy=(np.sqrt(0.2), 1),  xycoords='data',
                xytext=(-50, -30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    l = plt.axvline(x=np.sqrt(0.0), color='k', linestyle='--')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s OSS %s Coefficient of Variation" % (dayStr, oss.name))
    plt.xlabel('CoV')
    plt.ylabel(r'average $MiB/sec$ across OSTs')
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************

def do_spectrum(oss, mode, plot, ybound):
    maxRate = 2.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = oss.CPU.Values/maxCPU
    if (mode == 'Read') or (mode is None):
        rate = oss.Read.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode is None):
        rate = oss.Write.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Write')
    if mode == 'Both':
        rate = oss.OSS.Values/maxRate
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
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(oss.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, oss.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************

def do_action(args, oss):
    if (args.show_osss == True) or (args.show_osts == True):
        # We don't expect to do anything if we're just looking at the
        # available names.
        return
    if not args.mask is None:
        oss.doMask(args.mask)
    if args.both == True:
        mode = 'Both'
    elif args.read == args.write:
        mode = None
    elif args.read == True:
        mode = 'Read'
    else:
        mode = 'Write'
    if args.show_steps == True:
        oss.show(mode)
        return
    if not args.apportion is None:
        #oss.showStep(oss.Steps.getIndex(args.apportion))
        ax = oss.pieChart(oss.Steps.getIndex(args.apportion), mode)
        if ax is None:
            print "oss: Error - Failed to set plot"
            return
        if args.plot is None:
            plt.show()
        else:
            plt.savefig(args.plot)
        return
    if args.report == True:
        oss.report()
    if args.plot == "noplot":
        return
    if args.spectrum == True:
        do_spectrum(oss, mode, args.plot, args.ybound)
        return
    if args.x_correlate == True:
        do_xcorr(oss, mode=mode, plot=args.plot, ybound=args.ybound)
        return
    if args.plotSdevs == True:
        do_sdevs(oss, mode, args.plot, ymax=args.ybound)
        return
    do_plot(oss, mode=mode, plot=args.plot, ybound=args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    oss.py <opts>
    Options include:
    -a <step>   Display the apportionment of I/O among the OSTs at step '-a <step>'
    -b <begin>  Beginning time stamp
    -B          Show the results of read and writes added together
    -c <conf>   Path to configuration file
    -C          Plot the CPU utilization
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -l          Graph using lines (no matter how many data points)
    -m          Plot the cross correlation spectrum
    -M <mask>   Filter out samples based on this mask
                <mask> is a string of key=values pairs with keys:
                left, right, top, bottom
    -o <oss>    The name of the OSS to examine
    -p <file>   File name of .png file for graph
    -P          Calculate and plot the standard deviation across OSTs at each timestep
    -r          Print a report of statistics
    -R          Show the read rates on the graph
    -s          Show the list of OSSs
    -S          Show the OSTs on <oss>
    -v          Print debug messages
    -V          Print the version and exit
    -W          Show the write rates on the graph
    -x          Cross correlate with the CPU utilization info
    -X          Show the steps for values in the given bounding box (-M <mask>)
    -y <ymax>   Maximum value of the y-axis

    This module supports pulling data for all the OSTs of a given OSS
    from the LMT DB.
    """
    args = process_args(main=True)
    oss = do_main(args)
    do_action(args, oss)

