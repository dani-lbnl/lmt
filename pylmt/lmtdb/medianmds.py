#!/usr/bin/env python
# medianmds.py
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
#   Use the daily.data file to produce a summary graphic showing
# the 30 median for bytes read and bytes wrtten, and the day's values.

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
    parser.add_argument('-d', '--day', default=None, type=str, help='The date for the figure (yyyy-mm-dd)')
    parser.add_argument('-f', '--file', default=None, type=str, help='The file to use (default ./dailymds.data)')
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
    moved (read and write) per day. Plot on a scale calibrated to the size of
    memory (212 TB) with a y2 scale for size of file system (1 PB for each of
    scratch and scratch2).
    """
    Window = 30
    days = np.empty(len(dailyMDS), dtype=str)
    days = dailyMDS[:]['day']
    ops = np.zeros(len(dailyMDS), dtype=np.float64)
    ops = dailyMDS[:]['ops']
    if not args.day is None:
        end = 0
        # The days array strings have a leading '"' mark you need to skip.
        while (days[end][1:] != args.day) and (end+1 < len(days)):
            end += 1
    else:
        end = len(days) - 1
    if end < Window:
        start = 0
    else:
        start = end - 30
    start_date = mpl.dates.date2num(datetime.datetime.strptime(days[start][1:], "%Y-%m-%d"))
    end_date = mpl.dates.date2num(datetime.datetime.strptime(days[end][1:], "%Y-%m-%d")) + 1
    opsDay = ops[end]/(1024*1024)
    opsMedian = np.median(ops[start:end+1])/(1024*1024)
    scale = 100
    max = opsDay
    if opsMedian > max:
        max = opsMedian
    if max >  scale:
        scale = makeRound(max)
    fig = plt.figure()
    fig.subplots_adjust(bottom=0.2)
    ax = fig.add_subplot(111)
    green = (0.0, 1.0, 0.0)
    darkgreen = (0.0, 0.25, 0.0)
    ax.bar([0.25], [opsDay], width=0.25, color=green, label="today's ops")
    ax.bar([0.50], [opsMedian], width=0.25, color=darkgreen, label="30 day median")
    ax.set_ylabel('MDS Gig-Ops $(10^9)$')
    ax.set_title('%s %s %s metadata ops\nand median (30 day window)' % (args.host, args.fs, days[end][1:]) )
    ax.get_xaxis().set_ticks([])
    ax.set_xbound(lower=0.0, upper=1.0)
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
    scale = 1.0
    sign = 1
    if val < 0.0:
        sign = -1
        val = -val
    if val > 1:
        while val > 10.0:
            val /= 10.0
            scale *= 10.0
        if val > 5.0:
            return(10.0*scale*sign)
        if val > 2.0:
            return(5.0*scale*sign)
        if val > 1.0:
            return(2.0*scale*sign)
    # val must have been 1.0 or less
    return(scale*sign)

#*******************************************************************************

def doAction(args, dailyMDS):
    doMedian(args, dailyMDS)

#*******************************************************************************

if __name__ == "__main__":
    """
    medianmds.py <opts>
    Options include:
    -d <day>    The date for the figure (yyyy-mm-dd)
    -f <file>   The daily.data file to use
    -p <plot>   the file to save the plot in
    -h          A help message
    -v          Print debug messages
    -V          Print the version and exit



    """
    args = process_args(main=True)
    if not args is None:
        dailyMDS = doMain(args)
        if not dailyMDS is None:
            doAction(args, dailyMDS)
