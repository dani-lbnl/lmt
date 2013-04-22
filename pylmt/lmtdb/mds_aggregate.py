#!/usr/bin/env python
# mds_aggregate.py <opts>
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
#    This module supports putting MDS summary data back into the LMT DB.
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
#    print "mds_aggregate: backend = %s" % backend
    if backend != 'agg':
        mpl.use('Agg')
#else:
#    print "DISPLAY =", os.environ['DISPLAY']
import matplotlib.pyplot as plt
import matplotlib.pylab as plb
from mpl_toolkits.mplot3d import Axes3D
from pyLMT import MDS, LMTConfig, Timestamp

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
    parser.add_argument('-c', '--config', default=None, type=file, help='The configuration file to use for DB access')
    parser.add_argument('-e', '--end', default=None, type=str, help='The end of the time interval to be queried in seconds in epoch (default "now")')
    parser.add_argument('-f', '--fs', default=None, type=str, help='The (db name of the) file system of interest')
    parser.add_argument('-i', '--index', default=1, type=int, help='The file system index (# as listed in conf: default = 1) to query for data')
    parser.add_argument('-v', '--verbose', action='store_true', default=False, help='Print out extra details')
    parser.add_argument('-V', '--version', action='version', version='%(prog)s 0.2')
    args = parser.parse_args()
    if main == True:
        args = validate_args(args)
    return(args)

#*******************************************************************************
def validate_args(args):
    # This is foolish, but I have some sort of code in the Timestamp module
    # that wants it.
    return(args)

#*******************************************************************************
# callable main function for working interactively
def do_main(args):
    """
    args.begin - (string) Gives the date in yyyy-mm-dd hh:mm:ss format.
                   It will fill in todays year, month and day if left out.
                   hh:mm:ss will default to 00:00:00 as portions are left out.
         config - (file) The lmtrc config file telling how to get to the DB
         end - (string) As above giving the end of the data to be gathered.
         fs - (string) The dbname entry in the config file for the file system of interest.
         index - (int) The index of the file system of interest in the config file
         verbose - (boolean) Turn on debugging output
         version - (boolean) print the version string and exit
    """
    fsrc = LMTConfig.process_configuration(args)
    mds = MDS.MDS(host=fsrc['host'], fs=fsrc['name'])
    if args.verbose == True:
        mds.debug()
        mds.debug(module="Operation")
        #mds.debug(module="Timestamp")
    mds.opsFromDB(fsrc['conn'])
    (begin_ts, end_ts) = Timestamp.process_timestamps(args, fsrc)
    mds.getData(begin=begin_ts,
                end=end_ts)
    if mds.haveData == False:
        print "got no data from %s to %s" % (args.begin, args.end)
        return(None)
    mds.getCPU()
    return(mds)

#*******************************************************************************

def do_action(mds):
    insert = mds.insertHeader()
    if insert is None:
        print "mds_aggregate.do_action(): Error - Failed to determine mdsID"
        return
    needs_comma = False
    for sie in mds.Steps.Steps:
        string = mds.insertValues(sie)
        if string is None:
            continue
        if needs_comma == True:
            insert += ","
        insert += string
        needs_comma = True
    mds.doInsert(insert)
    return

#*******************************************************************************


if __name__ == "__main__":
    """
    mds_aggregate.py <opts>
    Options include:
    -b <begin>  Beginning time stamp
    -c <conf>   Path to configuration file
    -e <end>    Ending time stamp
    -f <fs>     The dbname for this filesystem in the lmtrc
    -h          A help message
    -i <index>  Index of the file system entry in the the config file
    -v          Print debug messages
    -V          Print the version and exit

    This module supprts putting MDS summary data back into the LMT DB.

    2012-02-13
    - version 0.1

    """
    args = process_args(main=True)
    if not args is None:
        mds = do_main(args)
        if not ((mds is None) or (mds.haveData == False)):
            do_action(mds)
