#!/usr/bin/env python
# dataave.py
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
#   Use the daily.data file to produce a summary graphic.


import os
import sys
import argparse
import numpy as np
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
import datetime
import time

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
    parser.add_argument('-b', '--begin', default=None, type=str, help='The beginning date for the histogram (yyyy-mm-dd)')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end date for the histogram (yyyy-mm-dd)')
    parser.add_argument('-f', '--file', default=None, type=str, help='The file to use (default ./daily.data)')
    parser.add_argument('-H', '--hist', action='store_true', default=False, help='Produce a histogram of the results')
    parser.add_argument('-M', '--median', action='store_true', default=False, help='Produce a grph of the median of the previous 30 days results')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Print out the read and write values')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        args.file = './daily.data'
    path = os.path.abspath(args.file)
    fs = os.path.dirname(path)
    args.fs = os.path.basename(fs)
    host = os.path.dirname(fs)
    args.host = os.path.basename(host)
    return(args)

#*******************************************************************************

def doMain(args):
    format = np.dtype([('day', 'S11'), ('hour', 'S11'), ('sie', np.uint),
                       ('read', np.float64), ('write', np.float64),
                       ('rRate', np.float64), ('wRate', np.float64)])
    try:
        daily = np.loadtxt(args.file, dtype=format)
    except:
        print "Failed to load from %s" % args.file
        return(None)
    return(daily)

#*******************************************************************************

def doHistogram(args, daily):
    days = np.empty(len(daily), dtype=str)
    days = daily[:]['day']
    read = np.zeros(len(daily), dtype=np.float64)
    read = daily[:]['read']
    write = np.zeros(len(daily), dtype=np.float64)
    write = daily[:]['write']
    if not args.begin is None:
        read = read[np.where(days >= args.begin)]
        write = write[np.where(days >= args.begin)]
    if not args.end is None:
        read = read[np.where(days <= args.end)]
        write = write[np.where(days <= args.end)]
    #read /= 1024*1024
    #write /= 1024*1024
    readMax = np.amax(read)
    max = readMax
    writeMax = np.amax(write)
    if writeMax > max:
        max = writeMax
    scale = makeRound(max)
    if not args.ybound == None:
        scale = args.ybound
    print readMax, writeMax, scale
    rHist,bins = np.histogram(read, bins=100, range=(0.0, scale))
    wHist,bins = np.histogram(write, bins=100, range=(0.0, scale))
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    width = 0.02
    x = bins[:-1]
    offset=0.0
    ax.bar(x+offset, rHist, width=width, color='r', label='read')
    offset += width
    ax.bar(x+offset, wHist, width=width, color='b', label='write')
    offset += width
    ax.set_ylabel('Number of Days')
    ax.set_xlabel('PiB')
    ax.set_title('%s %s Daily I/O Totals\nHistogram' % (args.host, args.fs) )
    plt.xticks(range(int(scale+1)))
    ax.legend()
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( "hist_" + args.plot )
    plt.cla()
    return

#*******************************************************************************

def doDaily(args, daily):
    """
    """
    # MemorySize in GB
    MemorySize = 212*1024
    # FileSystemSize in GB (each separately is about 1 PB)
    FileSystemSize = 1024*1024
    days = np.empty(len(daily), dtype=str)
    days = daily[:]['day']
    read = np.zeros(len(daily), dtype=np.float64)
    read = daily[:]['read']
    write = np.zeros(len(daily), dtype=np.float64)
    write = daily[:]['write']
    # The days array string have a leading '"' mark you need to skip.
    start_date = mpl.dates.date2num(datetime.datetime.strptime(days[0][1:], "%Y-%m-%d"))
    end_date = mpl.dates.date2num(datetime.datetime.strptime(days[-1][1:], "%Y-%m-%d")) + 1
    if not args.begin is None:
        read = read[np.where(days >= args.begin)]
        write = write[np.where(days >= args.begin)]
        start_date = mpl.dates.date2num(datetime.datetime.strptime(args.begin, "%Y-%m-%d"))
    if not args.end is None:
        read = read[np.where(days <= args.end)]
        write = write[np.where(days <= args.end)]
        end_date = mpl.dates.date2num(datetime.datetime.strptime(args.end, "%Y-%m-%d")) + 1
    dates = np.array(range(int(start_date), int(end_date)))
    # For some reason this operation side effects the days array, since in the other
    # functions the values have already been scaled. I don't get it.
    read /= FileSystemSize
    write /= FileSystemSize
    readMax = np.amax(read)
    max = readMax
    writeMax = np.amax(write)
    if writeMax > readMax:
        max = writeMax
    scale = makeRound(max)
    if args.report == True:
        print "read average = %f" % np.average(read)
        print "write average = %f" % np.average(write)
        readSort = np.sort(read)
        writeSort = np.sort(write)
        l = len(readSort)
        if (len(readSort) % 2) == 0:
            readMed = (readSort[l/2 -1] + readSort[l/2])/2
        else:
            readMed = readSort[l/2]
        l = len(writeSort)
        if (len(writeSort) % 2) == 0:
            writeMed = (writeSort[l/2 -1] + writeSort[l/2])/2
        else:
            writeMed = writeSort[l/2]
        print "read median = %f" % readMed
        print "write median = %f" % writeMed
        print "read maximum = %f" % readMax
        print "write maximum = %f" % writeMax
    if args.plot == "noplot":
        return
    if not args.ybound == None:
        scale = args.ybound
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    ax.plot_date(dates, read, 'r-', label='read')
    ax.plot_date(dates, write, 'b-', label='write')
    ax.set_ylabel('PB')
    ax.set_title('%s %s daily data moved' % (args.host, args.fs) )
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    ax.set_ybound(lower=0.0, upper=scale)
    ax.legend()
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( args.plot )
    plt.cla()
    return

#*******************************************************************************

def doMedian(args, daily):
    """
    Calculate and graph the 30 day running median for amounts of data
    moved (read and write) per day. Plot on a scale calibrated to the size of
    memory (212 TB) with a y2 scale for size of file system (1 PB for each of
    scratch and scratch2).
    """
    Window = 30
    # MemorySize in GB
    MemorySize = 212*1024
    # FileSystemSize in GB
    FileSystemSize = 1024*1024
    days = np.empty(len(daily), dtype=str)
    days = daily[:]['day']
    read = np.zeros(len(daily), dtype=np.float64)
    read = daily[:]['read']
    write = np.zeros(len(daily), dtype=np.float64)
    write = daily[:]['write']
    # The days array string have a leading '"' mark you need to skip.
    start_date = mpl.dates.date2num(datetime.datetime.strptime(days[0][1:], "%Y-%m-%d"))
    end_date = mpl.dates.date2num(datetime.datetime.strptime(days[-1][1:], "%Y-%m-%d")) + 1
    if not args.begin is None:
        read = read[np.where(days >= args.begin)]
        write = write[np.where(days >= args.begin)]
        start_date = mpl.dates.date2num(datetime.datetime.strptime(args.begin, "%Y-%m-%d"))
    if not args.end is None:
        read = read[np.where(days <= args.end)]
        write = write[np.where(days <= args.end)]
        end_date = mpl.dates.date2num(datetime.datetime.strptime(args.end, "%Y-%m-%d")) + 1
    dates = np.array(range(int(start_date), int(end_date)))
    readMedian = np.zeros(len(read), dtype=np.float64)
    writeMedian = np.zeros(len(write), dtype=np.float64)
    for end in range(len(read)):
        start = 0
        if end > Window:
            start = end - Window
        readMedian[end] = np.median(read[start:end+1])
        writeMedian[end] = np.median(write[start:end+1])
    #readMedian /= MemorySize
    #writeMedian /= MemorySize
    max = np.amax(readMedian)
    wmax = np.amax(writeMedian)
    if wmax > max:
        max = wmax
    scale = makeRound(max)
    if not args.ybound == None:
        scale = args.ybound
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    ax.plot_date(dates, readMedian, 'r-', label='read')
    ax.plot_date(dates, writeMedian, 'b-', label='write')
    ax.set_ylabel('fraction of memory')
    ax.set_title('%s %s median (30 day window)\nof daily data moved' % (args.host, args.fs) )
    plt.setp( ax.get_xticklabels(), rotation=30, horizontalalignment='right')
    ax.set_ybound(lower=0.0, upper=scale)
    ax.legend()
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( "median_" + args.plot )
    plt.cla()
    return

#*******************************************************************************

def makeRound(val):
    scale = 1
    sign = 1
    if val < 0:
        sign = -1
        val = -val
    while val > 10:
        val /= 10
        scale *= 10
    if val > 5:
        return(10*scale*sign)
    if val > 2:
        return(5*scale*sign)
    if val > 1:
        return(2*scale*sign)
    # I don't think this will ever happen
    return(scale*sign)

#*******************************************************************************

# This turned out to be not very popular
def doBoxplot(args, daily):
    days = np.empty(len(daily), dtype=str)
    days = daily[:]['day']
    times = np.zeros(len(daily), dtype=np.int32)
    times = daily[:]['sie']
    read = np.zeros(len(daily), dtype=np.float64)
    read = daily[:]['read']
    write = np.zeros(len(daily), dtype=np.float64)
    write = daily[:]['write']
    if args.begin is None:
        args.begin = days[0][1:11]
    if args.end is None:
        args.end = days[-1][1:11]
    beginSie = int(time.mktime(time.strptime(args.begin, "%Y-%m-%d")))
    endSie = int(time.mktime(time.strptime(args.end, "%Y-%m-%d")))
    read = read[np.where(np.logical_and((times >= beginSie),(times <= endSie)))]
    write = write[np.where(np.logical_and((times >= beginSie),(times <= endSie)))]
    read /= 1024.0
    write /= 1024.0
    r_quartiles = np.percentile(read, (25.0, 50.0, 75.0))
    scale = makeRound(r_quartiles[2])
    w_quartiles = np.percentile(write, (25.0, 50.0, 75.0))
    wscale = makeRound(w_quartiles[2])
    if wscale > scale:
        scale = wscale
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    left, width = .33, .33
    bottom, height = 0.0, 0.75
    right = left + width
    top = bottom + height
    ax.text(left, bottom, 'read',
            horizontalalignment='center',
            verticalalignment='top',
            transform=ax.transAxes)
    plt.axvline(x=0.33, ymin=r_quartiles[0]/scale, ymax=r_quartiles[2]/scale, linewidth=40, color='r')
    ax.text(left-0.06, r_quartiles[0]/scale, '1st q',
            horizontalalignment='right',
            verticalalignment='top',
            transform=ax.transAxes)
    ax.text(left-0.06, r_quartiles[2]/scale, '4th q',
            horizontalalignment='right',
            verticalalignment='bottom',
            transform=ax.transAxes)
    plt.axhline(y=r_quartiles[1], xmin=0.27, xmax=0.39, linewidth=2, color='k')
    ax.text(left-0.06, r_quartiles[1]/scale, 'median',
            horizontalalignment='right',
            verticalalignment='bottom',
            transform=ax.transAxes)
    ax.text(right, bottom, 'write',
            horizontalalignment='center',
            verticalalignment='top',
            transform=ax.transAxes)
    plt.axvline(x=0.66, ymin=w_quartiles[0]/scale, ymax=w_quartiles[2]/scale, linewidth=40, color='b')
    ax.text(right+0.06, w_quartiles[0]/scale, '1st q',
            horizontalalignment='left',
            verticalalignment='top',
            transform=ax.transAxes)
    ax.text(right+0.06, w_quartiles[2]/scale, '4th q',
            horizontalalignment='left',
            verticalalignment='bottom',
            transform=ax.transAxes)
    plt.axhline(y=w_quartiles[1], xmin=0.6, xmax=0.73, linewidth=2, color='k')
    ax.text(right+0.06, w_quartiles[1]/scale, 'median',
            horizontalalignment='left',
            verticalalignment='bottom',
            transform=ax.transAxes)
    ax.set_ylabel('TB')
    ax.set_ybound(lower = 0, upper = scale)
    plt.xticks(np.arange(2), ('', ''))
    ax.set_title('%s %s I/O totals' % (args.host, args.fs) )
    if args.plot is None:
        plt.show()
    else:
        plt.savefig( args.plot )
    plt.cla()
    return

#*******************************************************************************

def doAction(args, daily):
    doDaily(args, daily)
    if args.median == True:
        doMedian(args, daily)
    if args.hist == True:
        doHistogram(args, daily)

#*******************************************************************************

if __name__ == "__main__":
    """
    dataave.py <opts>
    Options include:
    -f <file>   The HDF5 file to use
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        daily = doMain(args)
        if not daily is None:
            doAction(args, daily)
