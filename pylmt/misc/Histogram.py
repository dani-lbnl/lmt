#!/usr/bin/env python
# Histogram.py
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
# Options include:
# -h       A help message
# -n <num> The number of values to plot.
#
# This module supports plotting graphs using numpy and matplotlib.
#
# 2012-01-21
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
#    print "Bulk: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import axes3d
from scipy import linspace, polyval, polyfit, sqrt, randn
import Timestamp

#*******************************************************************************
def process_args(main=False):
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-n', '--num', default=100, type=int, help='How many values to graph')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    if args.num <= 0:
        print "Histogram: You need to provide a positive integer"
        sys.exit(1)
    return(args)

#*******************************************************************************
def do_main(args):
    """
    args.num - the number of points
    """
    x = range(args.num)
    y = np.random.random(args.num)
    pair = (x, y)
    return(pair)

#*******************************************************************************
def do_action(args, pair):
    """
    I need to generate appropriate content here.
    """
    plt.xlabel('x')
    plt.ylabel(r'y')
    plt.title('x versus y')
    if args.plot == None:
        plt.show()
    else:
        plt.savefig(args.plot)

#*******************************************************************************
def timeSeries(ax, times, values, color, label=None, Ave=False):
    """
    Provide and axes object and times and values as from a TimeSeries object.
    Plot the time series values on the axes with suitably organized date values.
    This relies on the Timestamp module. Use the indicated color. Include the
    label if any. Also plot the average as a black dotted line if requested. Note
    that the values should already be scaled when delivered.
    """
    ltimes = len(times)
    lvalues = len(values)
    if (ltimes <= 1) or (ltimes != lvalues):
        print "Histogram.timeSeries(): Warning - %d times and %d values." (ltimes, lvalues)
        return
    # plot date takes a float array of days since epoc
    dates = np.array(times, dtype=np.float64)
    if ltimes <= 500:
        format = '-'
    else:
        # the ',' make the point one pixel
        format = ','
    if Timestamp.dst(times[0]) == True:
        tzAdjust = 7.0
    else:
        tzAdjust = 8.0
    dates = (dates - tzAdjust*60.0*60.0)/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    if Ave == True:
        ave = np.ones_like(dates)*np.average(values)
        ax.plot_date(dates, ave, fmt='--', xdate=True, ydate=False, color=color, label='ave', zorder=1)
    ax.plot_date(dates, values, fmt=format, xdate=True, ydate=False, color=color, label=label, zorder=0)

#*******************************************************************************
def percent(ax, times, values, color='k', label="% CPU", Ave=False):
    """
    Provide and axes object and times and values as from a CPU object.
    Create secondary axes with separately scaled y-axis. Plot the time series
    values on the new axes.
    This relies on the Timestamp module. Use the indicated color. Include the
    label if any. Also plot the average as a black dotted line if requested. Note
    that the values should already be scaled when delivered.
    """
    ltimes = len(times)
    lvalues = len(values)
    if (ltimes <= 1) or (ltimes != lvalues):
        print "Histogram.timeSeries(): Warning - %d times and %d values." (ltimes, lvalues)
        return(None, None)
    handles1,labels1 = ax.get_legend_handles_labels()
    # plot date takes a float array of days since epoc
    dates = np.array(times, dtype=np.float64)
    if ltimes <= 500:
        format = '-'
    else:
        # the ',' make the point one pixel
        format = ','
    if Timestamp.dst(times[0]) == True:
        tzAdjust = 7.0
    else:
        tzAdjust = 8.0
    dates = (dates - tzAdjust*60.0*60.0)/(24.0*60.0*60.0) + mpl.dates.date2num(datetime.date(1970,1,1))
    ax2 = ax.twinx()
    if Ave == True:
        ave = np.ones_like(dates)*np.average(values)
        ax2.plot_date(dates, ave, fmt='--', xdate=True, ydate=False, color=color, label='ave', zorder=1)
    ax2.plot_date(dates, values, fmt=format, xdate=True, ydate=False, color=color, label=label, zorder=0)
    ax2.set_ybound(lower = 0, upper = 100)
    ax2.set_ylabel(r'$\% CPU$')
    handles2,labels2 = ax2.get_legend_handles_labels()
    handles1 += handles2
    labels1 += labels2
    return(handles1, labels1)

#*******************************************************************************

def scatter(ax, xvals, yvals, color, label=None):
    """
    Produce a scatter plot of the value pairs.
    """
    xlen = len(xvals)
    ylen = len(yvals)
    if (xlen <= 1) or (xlen != ylen):
        print "Histogram.scatter(): Warning - problem with lengths (%d, %d)" % (xlen, ylen)
        return
    ax.plot(xvals, yvals, color+',', label=label)

#*******************************************************************************

if __name__ == "__main__":
    """
    Histogram.py <opts>
    Options include:
    -h       A help message
    -n <num> The number of values to plot
    This module supports plotting graphs using numpy and matplotlib.

    2012-01-21
    - version 0.1

    Todo:
    - Update this todo list :)

    """
    args = process_args(main=True)
    pair = do_main(args)
    do_action(args, pair)
