#!/usr/bin/env python
# metadata_day.py <opts>
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
# -c <conf>   Path to configuration file
# -d <day>    The for which data is to be gathered
# -f <fs>     The dbname for this filesystem in the lmtrc
# -h          A help message
# -i <index>  Index of the file system entry in the the config file
# -m <mds_op> The MDS operation to examine
# -M <mds_op> The MDS operation to highlight
# -p <file>   file name of .png file for graph
# -v          Print debug messages
# -r          Produce a report on the observed values
# -V          Print the version and exit
# -y <ymax>   Maximum value of the y-axis
#
#   This module supports pulling MDS data from the LMT DB.
#
# 2011-10-20
# - version 0.1
#
# 2011-12-02
# - Allow for multiple -m <op> arguments
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
#    print "MDS: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import Axes3D
from scipy import linspace, polyval, polyfit, sqrt, randn
from pyLMT import MDS, LMTConfig, Timestamp, TimeSteps

#*******************************************************************************
# Support for basic calling conventions
def process_args():
    """
    The command line arguments needed for operating the MDS class as
    a simple script.
    On success return the args object with validatated entries for
    those thing required by the __man__ script.
    """
    mpl_ver = string.split( mpl.__version__, '.')
    parser = argparse.ArgumentParser(description='Access an LMT DB')
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-d', '--day', default=None, type=str, help='The day to process "yyyy-mm-dd"')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-m', '--mds_op', action='append', default=None, type=str, help='Name of the MDS operation to examine. Multiple ops are allowed.')
    parser.add_argument('-M', '--hilite_op', default=None, type=str, help='One of the MDS ops can be called out to be highlighted in correlation graphs')
    parser.add_argument('-p', '--plot', default=None, type=str, help='The name of the file to save the plot in')
    parser.add_argument('-r', '--report', action='store_true', default=False, help='Produce a report on the observed values')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    parser.add_argument('-y', '--ybound', default=None, type=float, help='Set the y-axis upper bound to this value')
    parser.add_argument('-x', '--x_correlate', action='store_true', default=False, help='Scatter plot of CPU utilization vs. -m <op>')
    args = parser.parse_args()
    if args.day == None:
        print "Please provide a day for which to process yyyy-mm-dd"
        sys.exit(1)
    if (args.mds_op == None) or (len(args.mds_op) == 0):
        if args.hilite_op == None:
            print "MDS: Error - No ops have been specified"
        else:
            args.mds_op = [args.hilite_op]
    if (args.x_correlate == True) and ((len(args.mds_op) != 1) or (args.mds_op[0] == "cpu")):
        print "MDS: If you want a cross correlation (-x) you need to provide exactly one other operation (not -m cpu)"
        sys.exit()
    if args.x_correlate == True:
        args.mds_op += ["cpu"]
    return(args)

#*******************************************************************************

print "This code is out of date. Revise before using."
sys.exit(1)
args = process_args()
fsrc = LMTConfig.process_configuration(args)
mds = MDS.MDS(host=fsrc['host'], fs=fsrc['name'])
if args.verbose == True:
    mds.debug()
    mds.debug(module="Operation")
    #mds.debug(module="Timestamp")
mds.opsFromDB(fsrc['conn'])
(beginTimestamp, endTimestamp) = Timestamp.day(args, fsrc)
Steps = TimeSteps.TimeSteps()
Steps.getTimeSteps(beginTimestamp, endTimestamp, fsrc['conn'])
mds.setSteps(Steps)
mds.getData()
if mds.haveData == False:
    print "got no data from %s to %s" % (args.begin, args.end)
    sys.exit(0)
if args.report == True:
    mds.report()
#print "%d steps" % mds.Steps.steps()
if args.x_correlate == True:
    if args.hilite_op == None:
        args.hilite_op = args.mds_op[0]
    if (args.plot == None) or (args.plot == "noplot"):
        MDS.do_xcorr(mds, plot=None, ybound=args.ybound, hilite=args.hilite_op)
    else:
        MDS.do_xcorr(mds, plot=args.plot, ybound=args.ybound, hilite=args.hilite_op)
    sys.exit(0)
if args.plot == "noplot":
    sys.exit(0)
if args.plot == None:
    mds.plot('show', ymax=args.ybound)
else:
    mds.plot(args.plot, ymax=args.ybound)
