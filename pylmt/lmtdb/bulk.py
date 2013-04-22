#!/usr/bin/env python
# bulk.py
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
# bulk.py
# Options include:
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
# -M <mask>   Filter out samples of the spectrum based on this mask
#                <mask> is a string of key=values pairs with keys:
#                left, right, top, bottom
# -p <file>   File name of .png file for graph
# -P          Calculate and plot the standard deviation across OSTs at each timestep
# -r          Print a report of statistics
# -R          Show the read rates on the graph
# -v          Print debug messages
# -V          Print the version and exit
# -W          Show the write rates on the graph
# -x          Cross correlate with the CPU utilization info
# -y <ymax>   Maximum value of the y-axis
#
#   This module supports pulling data for all OSSs and OSTs from the LMT DB.
#
# 2012-01-02
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
    parser.add_argument('-B', '--both', action='store_true', default=False, help='Plot the sum of the read and write rates')
    parser.add_argument('-c', '--config', default='/project/projectdirs/pma/lmt/etc/lmtrc', type=file, help='The configuration file to use for DB access')
    parser.add_argument('-C', '--cpu', action='store_true', default=False, help='Plot the CPU utilization')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-l', '--lines', action='store_true', default=False, help='Graph using lines (no matter how many data points)')
    parser.add_argument('-m', '--spectrum', action='store_true', default=False, help='Plot the cross-correlation spectrum')
    parser.add_argument('-M', '--mask', default=None, type=str, help='Filter out samples of the spectrum based on the mask. eg. "left=0.0,right=0.1" only shows the values with CPU utilization up  to 0.1. "bottom=0.0,top=0.1" does the same for data rate')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-P', '--plotSdevs', action='store_true', default=False, help='Calculate and plot the standard deviation across OSTs at each timestep')
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
        print "bulk: Try doing either -B (both) or one or both of -R (read) and -W (write)"
        sys.exit(1)
    if (args.both == False) and (args.read == False) and (args.write == False):
        args.read = True
        args.write = True
    if not args.mask is None:
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
         lines - (boolean) Graph using lines (no matter how many data points)
         mask - (key=value:keys in {mincpu, maxcpu, minval, maxval}) mask values outside the given range
         plot - (string) The name of the file to which the graph should be saved.
                  'noplot' is allowed if you just want a report.
         plotSdevs (boolean) Calculate and plot the standard deviation across OSTs at each timestep
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
    # - return the bulk object
    fsrc = LMTConfig.process_configuration(args)
    if fsrc is None:
        return None
    B = Bulk.Bulk(fsrc['name'])
    if args.verbose == True:
        B.debug()
    B.getOSSs(fsrc['conn'])
    (beginTimestamp, endTimestamp) = Timestamp.process_timestamps(args, fsrc)
    Steps = TimeSteps.TimeSteps()
    Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
    B.setSteps(Steps)
    if (B.Steps is None) or (B.Steps.steps() == 0):
        print "bulk: Warning - No steps from FS %s" % B.name
        sys.exit(1)
    B.getData()
    if (args.cpu == True) or (args.x_correlate == True):
        B.getCPU()
    return(B)

#*******************************************************************************
def do_plot(B, mode=None, plot=None, ybound=None,
            scale=1024.0*1024.0, withCPU=True):
    if args.lines == True:
        format = '-'
    else:
        format = None
    fig = plt.figure()
    ax = fig.add_subplot(111)
    steps = B.Steps.Steps
    ymax = 0
    np.set_printoptions(threshold='nan')
    if mode == 'Both':
        values = B.Bulk.Values/scale
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='read and write',
                         Ave=True, format=format)
    elif mode is None:
        values = B.Read.Values/scale
        #print "read: ", values
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'r', label='read',
                         Ave=True, format=format)
        values = B.Write.Values/scale
        #print "write: ", values
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='write',
                         Ave=True, format=format)
    elif mode == 'Read':
        values = B.Read.Values/scale
        #print "read: ", values
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'r', label='read',
                         Ave=True, format=format)
    else:
        values = B.Write.Values/scale
        #print "write: ", values
        max = np.max(values)
        if max > ymax:
            ymax = max
        Graph.timeSeries(ax, steps, values, 'b', label='write',
                         Ave=True, format=format)
    plt.xlabel('time')
    plt.ylabel(r'$MiB/sec$')
    if (withCPU == True) and (not B.CPU is None):
        values = np.array(B.CPU.Values)
        #values[np.where(values.mask==True)] = 0.0
        (handles, labels) = Graph.percent(ax, steps, values, 'k',
                                          label='pct CPU', Ave=True)
        # insert bogus here for testing
        if (not handles is None) and (not labels is None):
            plt.legend(handles, labels)
        else:
            print "bulk.do_plot(): Warning - Plotting CPU utilization failed."
    else:
        plt.legend()
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    dayStr = time.strftime("%Y-%m-%d", time.localtime(B.begin.sie))
    plt.title("%s %s aggregate I/O" % (dayStr, B.name))
    if ybound is None:
        ybound = ymax
    ax.set_ybound(lower = 0, upper = ybound)
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    plt.cla()
    # bogus for above testing
    #handles,labels = ax.get_legend_handles_labels()
    # plot date takes a float array of days since epoc
    #dates = np.array(steps, dtype=np.float64)
    #dates = (dates - 8.0*60.0*60.0)/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    #ax2 = ax.twinx()
    #ave = np.ones_like(dates)*np.average(values)
    #ax2.plot_date(dates, ave, fmt='--', xdate=True, ydate=False, color='k', label='ave', zorder=1)
    #ax2.plot_date(dates, values, fmt='-', xdate=True, ydate=False, color='k', label='pct CPU', zorder=0)
    #ax2.set_ybound(lower = 0, upper = 100.0)
    #ax2.set_ylabel("pct CPU")
    #handles2,labels2 = ax2.get_legend_handles_labels()
    #handles += handles2
    #labels += labels2
    # This far for the bogus debug

#*******************************************************************************

def do_xcorr(B, mode=None, plot=None, ybound=None, scale=1024*1024):
    """
    The scatter plot of the aggregate I/O rates versus average CPU utilization
    is less illumination than the composite of all the individual OSS scatter
    plots. I don't call on this one, but it is still here and available.
    """
    if not ((mode == 'Read') or (mode == 'Write') or (mode == 'Both') or (mode is None)):
        print "bulk.do_xcorr: Error - Unrecognized mode %s" % mode
        return
    if B.CPU is None:
        print "bulk.do_xcorr(): Error - There is no CPU utilization data for %s" % B.name
        return
    if ((B.Read is None) or (B.Write is None) or (B.bulk is None)):
        print "bulk.do_xcorr(): Error - There is no data"
        return(None)
    if ybound is None:
        if mode == 'Read':
            ymax = B.Read.getMax()/scale
        elif mode == 'Write':
            ymax = B.Write.getMax()/scale
        elif mode == 'Both':
            ymax = B.bulk.getMax()/scale
        else:
            readMax = B.Read.getMax()/scale
            writeMax = B.Write.getMax()/scale
            if readMax > writeMax:
                ymax = readMax
            else:
                ymax = writeMax
        ybound = ymax
    fig = plt.figure()
    ax = fig.add_subplot(111)
    if (mode is None) or (mode == 'Read'):
        Graph.scatter(ax, B.CPU.Values, B.Read.Values/scale, 'r', label="read")
    if (mode is None) or (mode == 'Write'):
        Graph.scatter(ax, B.CPU.Values, B.Write.Values/scale, 'b', label="write")
    if mode == 'Both':
        Graph.scatter(ax, B.CPU.Values, B.bulk.Values/scale, 'b', label="read+write")
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(B.begin.sie))
    plt.title("%s bulk %s activity vs %%CPU" % (dayStr, B.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)

#*******************************************************************************

def do_composite_xcorr(B, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    if ybound is None:
        ymax = 0.0
        for oss in B.OSSs:
            if mode == 'Read':
                ossMax = oss.Read.getMax()/scale
            elif mode == 'Write':
                ossMax = oss.Write.getMax()/scale
            elif mode == 'Both':
                ossMax = oss.OSS.getMax()/scale
            else:
                readMax = oss.Read.getMax()/scale
                writeMax = oss.Write.getMax()/scale
                if readMax > writeMax:
                    ossMax = readMax
                else:
                    ossMax = writeMax
            if ossMax > ymax:
                ymax = ossMax
        ybound = ymax
    if ybound <= 0.0:
        print "bulk.do_composite_xcorr(): Warning - Failed to determine y-scale"
        return
    fig = plt.figure()
    ax = fig.add_subplot(111)
    handles = None
    labels = None
    for oss in B.OSSs:
        if (mode is None) or (mode == 'Read'):
            Graph.scatter(ax, oss.CPU.Values, oss.Read.Values/scale, 'r', label='read')
        if (mode is None) or (mode == 'Write'):
            Graph.scatter(ax, oss.CPU.Values, oss.Write.Values/scale, 'b', label='write')
        if mode == 'Both':
            Graph.scatter(ax, oss.CPU.Values, oss.OSS.Values/scale, 'b', label='read+write')
        if (handles is None) and (labels is None):
            (handles, labels) = ax.get_legend_handles_labels()
    plt.legend(handles, labels)
    ax.set_xbound(lower = 0, upper = 100)
    ax.set_ybound(lower = 0, upper = ybound)
    dayStr = time.strftime("%Y-%m-%d", time.localtime(B.begin.sie))
    plt.title("%s Composite %s OSS activity vs %%CPU" % (dayStr, B.name))
    ax.set_xlabel(r"$\%$ CPU")
    ax.set_ylabel(r"$MB/sec$")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)

#*******************************************************************************

def do_sdevs(B, mode=None, plot=None, ybound=None, scale=1024.0*1024.0):
    fig = plt.figure()
    ax = fig.add_subplot(111)
    for oss in B.OSSs:
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
    if not ybound is None:
        ax.set_ybound(lower = 0, upper = ybound)
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
    dayStr = time.strftime("%Y-%m-%d", time.localtime(B.begin.sie))
    plt.title("%s %s coefficinet of variation vs average rate" % (dayStr, B.name))
    plt.xlabel('CoV')
    plt.ylabel(r'average $MiB/sec$ across OSTs (per OSS)')
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************

def do_composite_spectrum(B, mode, plot, ybound):
    maxRate = 2.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    read = None
    write = None
    both = None
    for oss in B.OSSs:
        cpu = oss.CPU.Values/maxCPU
        if (mode == 'Read') or (mode is None):
            rate = oss.Read.Values/maxRate
            ratio = np.zeros_like(rate)
            ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
            spectrum = np.arctan(ratio)
            hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
            if read is None:
                read = hist
            else:
                read += hist
        if (mode == 'Write') or (mode is None):
            rate = oss.Write.Values/maxRate
            ratio = np.zeros_like(rate)
            ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
            spectrum = np.arctan(ratio)
            hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
            if write is None:
                write = hist
            else:
                write += hist
        if mode == 'Both':
            rate = oss.OSS.Values/maxRate
            ratio = np.zeros_like(rate)
            ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
            spectrum = np.arctan(ratio)
            hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
            if both is None:
                both = hist
            else:
                both += hist
    if (mode == 'Read') or (mode is None):
        ax.plot(bins[1:-1], read[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode is None):
        ax.plot(bins[1:-1], write[1:], 'b-', label='Write')
    if mode == 'Both':
        ax.plot(bins[1:-1], both[1:], 'b-', label='Both')
    l = plt.axvline(x=np.pi/2.0, color='k', linestyle='--')
    ax.annotate(r'$\pi/2$', xy=(np.pi/2, 1),  xycoords='data',
                xytext=(-50, 30), textcoords='offset points',
                arrowprops=dict(arrowstyle="->")
                )
    ax.set_xbound(lower = 0, upper = 1.6)
    if not ybound is None:
        ax.set_ybound(lower = 0, upper=ybound)
    ax.legend()
    dayStr = time.strftime("%Y-%m-%d", time.localtime(B.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, B.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************

def do_spectrum(B, mode, plot, ybound):
    maxRate = 50.0*1024*1024*1024
    maxCPU = 100.0
    fig = plt.figure()
    ax = fig.add_subplot(111)
    cpu = B.CPU.Values/maxCPU
    if (mode == 'Read') or (mode is None):
        rate = B.Read.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'r-', label='Read')
    if (mode == 'Write') or (mode is None):
        rate = B.Write.Values/maxRate
        ratio = np.zeros_like(rate)
        ratio = (rate[cpu > 0.0])/(cpu[cpu > 0.0])
        spectrum = np.arctan(ratio)
        hist, bins = np.histogram(spectrum, bins=100, range=(0.0, 1.6))
        ax.plot(bins[1:-1], hist[1:], 'b-', label='Write')
    if mode == 'Both':
        rate = B.bulk.Values/maxRate
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
    dayStr = time.strftime("%Y-%m-%d", time.localtime(B.begin.sie))
    plt.title("%s %s rate/CPU spectrum" % (dayStr, B.name))
    ax.set_xlabel(r"arctan($((MB/s)/2000)/(\%$ CPU))")
    ax.set_ylabel(r"count")
    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
    return

#*******************************************************************************

def do_action(args, B):
    if not args.mask is None:
        B.doMask(args.mask)
    if args.report == True:
        B.report()
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
        do_composite_spectrum(B, mode, args.plot, args.ybound)
        return
    if args.x_correlate == True:
        if not args.mask is None:
            for oss in B.OSSs:
                oss.doMask(args.mask)
        do_composite_xcorr(B, mode=mode, plot=args.plot, ybound=args.ybound)
        return
    if args.plotSdevs == True:
        do_sdevs(B, mode, args.plot, ybound=args.ybound)
        return
    do_plot(B, mode=mode, plot=args.plot, ybound=args.ybound)

#*******************************************************************************

if __name__ == "__main__":
    """
    bulk.py <opts>
    Options include:
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
        B = do_main(args)
        if not B is None:
            do_action(args, B)

