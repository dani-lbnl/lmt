#!/usr/bin/env python
# mdsave.py
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
#   Use the dailymds.data file to produce a summary graphic.

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
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Print out the read and write values')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************

def validate_args(args):
    if args.file is None:
        args.file = './dailymds.data'
    path = os.path.abspath(args.file)
    fs = os.path.dirname(path)
    args.fs = os.path.basename(fs)
    host = os.path.dirname(fs)
    args.host = os.path.basename(host)
    return(args)

#*******************************************************************************

def doMain(args):
    format = np.dtype([('day', 'S11'), ('hour', 'S11'), ('sie', np.uint),
                       ('ops', np.float64), ('opsRate', np.float64)])
    try:
        dailyMDS = np.loadtxt(args.file, dtype=format)
    except:
        print "Failed to load from %s" % args.file
        return(None)
    return(dailyMDS)

#*******************************************************************************

def doMedian(args, dailyMDS):
    """
    Calculate and graph the 30 day running median for amounts of data
    moved (ops and write) per day. Plot on a scale calibrated to the size of
    memory (212 TB) with a y2 scale for size of file system (1 PB for each of
    scratch and scratch2).
    """
    Window = 30
    days = np.empty(len(dailyMDS), dtype=str)
    days = dailyMDS[:]['day']
    # The days array string have a leading '"' mark you need to skip.
    start = 0
    if not args.begin is None:
        while (start < len(days)) and (days[start][1:] != args.begin):
            start += 1
        if start == len(days):
            print "mdsave.py:doMedian() %s does not appear to be on file" % args.begin
            return
    end = len(days) - 1
    if not args.end is None:
        while (end >= 0) and (days[end][1:] != args.end):
            end -= 1
        if end < 0:
            print "mdsave.py:doMedian() %s does not appear to be on file" % args.end
            return
        if (not args.begin is None) and (start >= end):
            print "mdsave.py:doMedian() %s is not before %s" % (args.begin, args.end)
    start_date = mpl.dates.date2num(datetime.datetime.strptime(days[start][1:], "%Y-%m-%d"))
    end_date = mpl.dates.date2num(datetime.datetime.strptime(days[end][1:], "%Y-%m-%d")) + 1
    ops = np.zeros(len(dailyMDS), dtype=np.float64)
    ops = dailyMDS[start:end+1]['ops']
    dates = np.array(range(int(start_date), int(end_date)))
    opsMedian = np.zeros(end-start+1, dtype=np.float64)
    for index in range(start, end+1):
        first = 0
        if index - first > Window:
            first = index - Window
        opsMedian[index] = np.median(ops[first:index+1])
    opsMedian /= 1024*1024
    max = np.amax(opsMedian)
    scale = 100
    if max > scale:
        scale = makeRound(max)
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    ax.plot_date(dates[start:end+1], opsMedian, 'k-', label='ops')
    ax.set_ylabel('Metadata Meg Ops ($10^6$)')
    ax.set_title('%s %s median (30 day window)\nof daily number of ops' % (args.host, args.fs) )
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

def doAction(args, dailyMDS):
    doMedian(args, dailyMDS)

#*******************************************************************************

if __name__ == "__main__":
    """
    mdsave.py <opts>
    Options include:
    -f <file>   The HDF5 file to use
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        dailyMDS = doMain(args)
        if not dailyMDS is None:
            doAction(args, dailyMDS)
